/*
   Copyright The containerd Authors.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package stdio

import (
	"bytes"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// pipeSource adapts one end of an os.Pipe to the Source interface. A blocking
// os.File read cannot be cancelled by Close on the same file, so a pump
// goroutine owns all reads into an internal buffer guarded by a condition
// variable. Read waits on the condition and drains whatever is available,
// mirroring the real fifo adapter's semantics (including partial reads) while
// staying platform independent and never losing an already-read chunk.
type pipeSource struct {
	r *os.File

	mu      sync.Mutex
	cond    *sync.Cond
	buf     bytes.Buffer
	readErr error
	aborted bool
	closed  bool
	done    chan struct{}
}

func newPipeSource(r *os.File) *pipeSource {
	p := &pipeSource{r: r, done: make(chan struct{})}
	p.cond = sync.NewCond(&p.mu)
	go p.pump()
	return p
}

func (p *pipeSource) pump() {
	defer close(p.done)
	tmp := make([]byte, 4096)
	for {
		n, err := p.r.Read(tmp)
		p.mu.Lock()
		if n > 0 {
			p.buf.Write(tmp[:n])
		}
		if err != nil {
			p.readErr = err
		}
		p.cond.Broadcast()
		p.mu.Unlock()
		if err != nil {
			return
		}
	}
}

func (p *pipeSource) Read(b []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	for {
		if p.aborted {
			return 0, errDrainAborted
		}
		if p.buf.Len() > 0 {
			return p.buf.Read(b)
		}
		if p.readErr != nil {
			return 0, p.readErr
		}
		p.cond.Wait()
	}
}

func (p *pipeSource) Unblock() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.aborted || p.closed {
		return nil
	}
	p.aborted = true
	p.cond.Broadcast()
	return nil
}

func (p *pipeSource) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	if !p.aborted {
		p.aborted = true
	}
	p.cond.Broadcast()
	p.mu.Unlock()

	err := p.r.Close()
	<-p.done
	return err
}

// pipeWriter is the test stand-in for the container-side writer.
type pipeWriter struct{ w *os.File }

func (p *pipeWriter) Write(b []byte) (int, error) { return p.w.Write(b) }
func (p *pipeWriter) Close() error                { return p.w.Close() }

// recordingSink captures everything the relay fails over or sends by default.
type recordingSink struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *recordingSink) Write(b []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(b)
}
func (s *recordingSink) Close() error { return nil }
func (s *recordingSink) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

// fakeDownstream is a deterministic in-memory client. While enabled every
// Write succeeds and the bytes are recorded; after Disconnect the next Write
// returns an error and the relay must fail the data over to its sink.
type fakeDownstream struct {
	mu       sync.Mutex
	enabled  bool
	received bytes.Buffer
}

func (d *fakeDownstream) Write(b []byte) (int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.enabled {
		return 0, io.ErrClosedPipe
	}
	return d.received.Write(b)
}

func (d *fakeDownstream) connect() {
	d.mu.Lock()
	d.enabled = true
	d.mu.Unlock()
}

func (d *fakeDownstream) disconnect() {
	d.mu.Lock()
	d.enabled = false
	d.mu.Unlock()
}

func (d *fakeDownstream) String() string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.received.String()
}

func quietLogger() *log.Logger {
	return log.New(io.Discard, "", 0)
}

func newPipeRelay(t *testing.T, name string) (*relay, *pipeWriter, *recordingSink) {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	sink := &recordingSink{}
	rl := newRelay(name, newPipeSource(r), sink, quietLogger())
	return rl, &pipeWriter{w: w}, sink
}

// setDownstreamForTest attaches a client and fails the test if the stream is
// not actually accepting downstreams.
func (r *relay) setDownstreamForTest(w io.Writer) error {
	r.setDownstream(w)
	if got := r.getDownstream(); got != w {
		return io.ErrShortWrite
	}
	return nil
}

// waitFor polls cond until it reports done or the deadline elapses.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// TestRelayClientDisconnectKeepsContainerWriting covers: a client disconnects,
// the container keeps writing without a broken pipe and the data lands in the
// log sink.
func TestRelayClientDisconnectKeepsContainerWriting(t *testing.T) {
	rl, containerW, sink := newPipeRelay(t, "stdout")
	defer rl.source.Close()

	client := &fakeDownstream{}
	client.connect()
	rl.setDownstream(client)

	if _, err := containerW.Write([]byte("to-client-1\n")); err != nil {
		t.Fatalf("container write with client attached: %v", err)
	}
	waitFor(t, "first client data", func() bool {
		return client.String() == "to-client-1\n"
	})

	// Client goes away. Detaching must not close the source end, and the
	// container's next writes must succeed (no EPIPE/SIGPIPE at the pipe the
	// container owns).
	client.disconnect()
	rl.setDownstream(nil)

	const disconnected = "after-disconnect\n"
	for i := 0; i < 5; i++ {
		if _, err := containerW.Write([]byte(disconnected)); err != nil {
			t.Fatalf("container write after disconnect failed: %v", err)
		}
	}
	waitFor(t, "disconnected data in sink", func() bool {
		return sink.String() == strings.Repeat(disconnected, 5)
	})

	// A new client can attach later and receive subsequent data.
	client2 := &fakeDownstream{}
	client2.connect()
	rl.setDownstream(client2)
	if _, err := containerW.Write([]byte("to-client-2\n")); err != nil {
		t.Fatalf("container write to new client: %v", err)
	}
	waitFor(t, "new client data", func() bool {
		return client2.String() == "to-client-2\n"
	})

	// End the stream cleanly: closing the writer yields EOF and the drain
	// reports the full tail.
	containerW.Close()
	rl.markExited()
	res := rl.drain(time.Second)
	if !res.EOF || res.Truncated {
		t.Fatalf("expected clean EOF drain, got %+v", res)
	}
	if got := sink.String(); got != strings.Repeat(disconnected, 5) {
		t.Fatalf("sink content mismatch: %q", got)
	}
}

// TestRelayDownstreamWriteFailureFailsOver covers a client whose write end
// errors mid-stream: the chunk must still reach the sink and the relay must
// not die.
func TestRelayDownstreamWriteFailureFailsOver(t *testing.T) {
	rl, containerW, sink := newPipeRelay(t, "stdout")

	clientR, clientW, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	rl.setDownstream(clientW)

	// Break the client side abruptly: close the read end so writes to
	// clientW fail with EPIPE/ErrClosedPipe.
	clientR.Close()
	if _, err := containerW.Write([]byte("fail-over-payload")); err != nil {
		t.Fatalf("container write: %v", err)
	}

	// Give the relay a moment to process and detach.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if sink.String() == "fail-over-payload" {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := sink.String(); got != "fail-over-payload" {
		t.Fatalf("expected fail-over to sink, got %q", got)
	}
	clientW.Close()

	containerW.Close()
	rl.markExited()
	res := rl.drain(time.Second)
	if !res.EOF {
		t.Fatalf("expected EOF drain, got %+v", res)
	}
}

// TestRelayLargeWriteLastLineIntact covers: the process writes a lot of data
// and exits immediately; the drain must keep running after the exit event and
// the log must contain the final line in full.
func TestRelayLargeWriteLastLine(t *testing.T) {
	rl, containerW, sink := newPipeRelay(t, "stdout")

	const chunk = "0123456789-abcdefghijklmnopqrstuvwxyz-line\n"
	const lines = 2000
	wantTail := "FINAL-LINE-must-survive-exit\n"

	go func() {
		buf := make([]byte, 0, len(chunk)*100)
		for i := 0; i < lines; i++ {
			buf = append(buf, chunk...)
			if len(buf) >= 4096 {
				_, _ = containerW.Write(buf)
				buf = buf[:0]
			}
		}
		if len(buf) > 0 {
			_, _ = containerW.Write(buf)
		}
		// Exit immediately after the last line: the exit event may arrive
		// before the relay has read everything.
		_, _ = containerW.Write([]byte(wantTail))
		_ = containerW.Close()
	}()

	// Exit notification races the copy; mark exited right away. The drain
	// must still consume the whole pipe up to EOF.
	rl.markExited()
	res := rl.drain(5 * time.Second)
	if !res.EOF || res.Truncated {
		t.Fatalf("expected full EOF drain, got %+v", res)
	}

	got := sink.String()
	wantLen := lines*len(chunk) + len(wantTail)
	if len(got) != wantLen {
		t.Fatalf("expected %d bytes, got %d", wantLen, len(got))
	}
	if got[len(got)-len(wantTail):] != wantTail {
		t.Fatalf("last line truncated: tail=%q", got[len(got)-80:])
	}
}

// TestRelayDrainTimeoutWhenGrandchildHoldsWriteEnd covers: a descendant keeps
// the write end open after the main process exits, so EOF never comes. The
// bounded drain must end on the timeout and report truncation instead of
// waiting forever.
func TestRelayDrainTimeoutWhenGrandchildHoldsWriteEnd(t *testing.T) {
	rl, containerW, sink := newPipeRelay(t, "stdout")

	if _, err := containerW.Write([]byte("before-exit\n")); err != nil {
		t.Fatalf("write: %v", err)
	}
	// Simulate the grandchild inheriting the fd: containerW stays open for
	// the whole test.
	defer containerW.Close()

	rl.markExited()

	start := time.Now()
	res := rl.drain(200 * time.Millisecond)
	elapsed := time.Since(start)

	if !res.Truncated || res.EOF {
		t.Fatalf("expected truncated drain, got %+v", res)
	}
	if res.Reason == "" {
		t.Fatal("truncated result must carry a reason")
	}
	if elapsed >= time.Second {
		t.Fatalf("drain did not respect timeout, took %v", elapsed)
	}
	if sink.String() != "before-exit\n" {
		t.Fatalf("data already written before timeout must be consumed, got %q", sink.String())
	}
}

// TestRelayNoSinkDropsData covers the default discard fallback: with no client
// and no sink the relay keeps consuming without blocking the writer.
func TestRelayNoSinkDropsData(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	rl := newRelay("stdout", newPipeSource(r), nil, quietLogger())

	done := make(chan struct{})
	go func() {
		for i := 0; i < 100; i++ {
			if _, err := w.Write(make([]byte, 4096)); err != nil {
				t.Errorf("writer blocked/failed while discarded: %v", err)
				break
			}
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("writer blocked: relay with discard sink did not keep up")
	}

	w.Close()
	rl.markExited()
	res := rl.drain(time.Second)
	if !res.EOF {
		t.Fatalf("expected EOF, got %+v", res)
	}
}

// TestManagerStdinGateEOF covers the input-direction rule: while the gate is
// open and no client feeds data, the reader blocks; EOF arrives only after
// CloseStdin (or MarkExited).
func TestManagerStdinGateEOF(t *testing.T) {
	containerR, gateW, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}

	cfg := Config{ID: "t", Dir: t.TempDir(), OpenStdin: true, Logger: quietLogger()}
	m := newManagerWithStreams(cfg, &stream{
		name:        StreamStdin,
		stdinWriter: gateW,
	})

	// One container-side reader collects everything until EOF.
	type chunk struct {
		data []byte
		err  error
	}
	got := make(chan chunk, 4)
	go func() {
		buf := make([]byte, 16)
		for {
			n, err := containerR.Read(buf)
			cp := make([]byte, n)
			copy(cp, buf[:n])
			got <- chunk{data: cp, err: err}
			if err != nil {
				return
			}
		}
	}()

	// No data: the container-side read must block.
	select {
	case c := <-got:
		t.Fatalf("stdin read returned before data/EOF was sent: %q %v", c.data, c.err)
	case <-time.After(50 * time.Millisecond):
	}

	// Data flows while the gate is open.
	if _, err := m.WriteStdin([]byte("ping")); err != nil {
		t.Fatalf("WriteStdin: %v", err)
	}
	select {
	case c := <-got:
		if c.err != nil || string(c.data) != "ping" {
			t.Fatalf("expected ping, got %q %v", c.data, c.err)
		}
	case <-time.After(time.Second):
		t.Fatal("container did not receive stdin data")
	}

	// CloseStdin delivers EOF, never data.
	if err := m.CloseStdin(); err != nil {
		t.Fatalf("CloseStdin: %v", err)
	}
	select {
	case c := <-got:
		if c.err != io.EOF {
			t.Fatalf("expected EOF, got %q %v", c.data, c.err)
		}
	case <-time.After(time.Second):
		t.Fatal("container did not observe EOF after CloseStdin")
	}
	containerR.Close()
	if err := m.Close(); err != nil {
		t.Fatalf("manager close: %v", err)
	}
}

// TestManagerMarkExitedDrainsAndMergesTerminal covers terminal mode: stderr is
// merged into stdout and MarkExited drives every relay through a full drain.
func TestManagerMarkExitedDrainsAndMergesTerminal(t *testing.T) {
	outR, outW, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	sink := &recordingSink{}
	outRelay := newRelay(StreamStdout, newPipeSource(outR), sink, quietLogger())

	cfg := Config{
		ID:           "term",
		Dir:          t.TempDir(),
		Terminal:     true,
		OpenStdout:   true,
		OpenStderr:   true,
		DrainTimeout: time.Second,
		Logger:       quietLogger(),
	}
	m := newManagerWithStreams(cfg, &stream{
		name:   StreamStdout,
		source: newPipeSource(outR),
		relay:  outRelay,
	})

	// Terminal mode must expose no stderr stream.
	if _, ok := m.streams[StreamStderr]; ok {
		t.Fatal("terminal mode must not create a separate stderr stream")
	}

	go func() {
		_, _ = outW.Write([]byte("merged output\n"))
		_ = outW.Close()
	}()
	m.MarkExited()
	results := m.Drain()
	if len(results) != 1 || !results[0].EOF {
		t.Fatalf("expected one clean stdout drain, got %+v", results)
	}
	if sink.String() != "merged output\n" {
		t.Fatalf("got %q", sink.String())
	}
	if err := m.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}
