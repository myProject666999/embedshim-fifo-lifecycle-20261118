//go:build linux
// +build linux

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
	"errors"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// These tests exercise the adapter against real fifos. They only exist on
// Linux; on every other platform the adapter returns ErrUnsupported and these
// tests are not compiled at all.

func isFIFO(t *testing.T, path string) {
	t.Helper()
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatalf("lstat %s: %v", path, err)
	}
	if info.Mode()&os.ModeNamedPipe == 0 {
		t.Fatalf("%s is not a fifo: %v", path, info.Mode())
	}
}

// TestEnsureFIFOCreatesAndIsIdempotent covers: missing directory creation,
// fifo creation, and idempotent reuse of an existing fifo (inode preserved).
func TestEnsureFIFOCreatesAndIsIdempotent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "dir", "out")
	if err := EnsureFIFO(path, os.Getuid(), os.Getgid()); err != nil {
		t.Fatalf("EnsureFIFO: %v", err)
	}
	isFIFO(t, path)

	info1, _ := os.Lstat(path)
	if err := EnsureFIFO(path, os.Getuid(), os.Getgid()); err != nil {
		t.Fatalf("second EnsureFIFO: %v", err)
	}
	info2, _ := os.Lstat(path)
	if !os.SameFile(info1, info2) {
		t.Fatal("idempotent EnsureFIFO must keep the same inode")
	}
	if info2.Mode().Perm() != fifoMode {
		t.Fatalf("unexpected perm %v", info2.Mode().Perm())
	}
}

// TestEnsureFIFOReplacesRegularFile covers the existing-regular-file case.
func TestEnsureFIFOReplacesRegularFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out")
	if err := os.WriteFile(path, []byte("not a pipe"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := EnsureFIFO(path, os.Getuid(), os.Getgid()); err != nil {
		t.Fatalf("EnsureFIFO over regular file: %v", err)
	}
	isFIFO(t, path)
}

// TestEnsureFIFORejectsSymlink covers the symlink case.
func TestEnsureFIFORejectsSymlink(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "target")
	link := filepath.Join(dir, "link")
	if err := os.WriteFile(target, []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	err := EnsureFIFO(link, os.Getuid(), os.Getgid())
	if !errors.Is(err, ErrSymlinkNotAllowed) {
		t.Fatalf("expected ErrSymlinkNotAllowed, got %v", err)
	}
}

// TestResetFIFORemovesStaleEntries covers new-task reuse: an old fifo (and a
// stray regular file) must be unlinked and replaced by a fresh inode.
func TestResetFIFORemovesStaleEntries(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out")

	// Stale fifo.
	if err := EnsureFIFO(path, os.Getuid(), os.Getgid()); err != nil {
		t.Fatal(err)
	}
	info1, _ := os.Lstat(path)
	if err := ResetFIFO(path, os.Getuid(), os.Getgid()); err != nil {
		t.Fatalf("ResetFIFO over stale fifo: %v", err)
	}
	info2, _ := os.Lstat(path)
	if os.SameFile(info1, info2) {
		t.Fatal("ResetFIFO must replace the stale fifo inode")
	}
	isFIFO(t, path)

	// Stale regular file.
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("stale"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := ResetFIFO(path, os.Getuid(), os.Getgid()); err != nil {
		t.Fatalf("ResetFIFO over regular file: %v", err)
	}
	isFIFO(t, path)
}

// TestOpenReadEndDoesNotNeedWriter covers the deadlock-free ordering: the
// management read end opens successfully before any writer exists.
func TestOpenReadEndDoesNotNeedWriter(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out")
	if err := EnsureFIFO(path, os.Getuid(), os.Getgid()); err != nil {
		t.Fatal(err)
	}

	done := make(chan Source, 1)
	go func() {
		f, err := openReadEnd(path)
		if err != nil {
			t.Errorf("openReadEnd blocked/failed without writer: %v", err)
			done <- nil
			return
		}
		done <- f
	}()
	select {
	case f := <-done:
		if f == nil {
			t.Fatal("openReadEnd failed")
		}
		f.Close()
	case <-time.After(2 * time.Second):
		t.Fatal("openReadEnd deadlocked waiting for a writer")
	}
}

// TestOpenWriteEndDoesNotNeedReader covers the reverse ordering: opening the
// write end before the container's read side exists must not fail with ENXIO.
func TestOpenWriteEndDoesNotNeedReader(t *testing.T) {
	path := filepath.Join(t.TempDir(), "in")
	if err := EnsureFIFO(path, os.Getuid(), os.Getgid()); err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		w, err := openWriteEnd(path)
		if err != nil {
			done <- err
			return
		}
		_ = w.Close()
		done <- nil
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("openWriteEnd failed without reader: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("openWriteEnd deadlocked waiting for a reader")
	}
}

// TestStdinGateDeliversEOFOnRealFifo verifies the input contract end to end:
// with no client the container's stdin read blocks; data written through the
// manager is received; and CloseStdin produces a real EOF. The temporary read
// hold used to open the gate must have been released, otherwise the fifo would
// still have a reader and the container would block forever instead of EOF.
func TestStdinGateDeliversEOFOnRealFifo(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{
		ID:        "stdin-task",
		Dir:       dir,
		UID:       os.Getuid(),
		GID:       os.Getgid(),
		OpenStdin: true,
		Logger:    quietLogger(),
	}
	m, err := Open(cfg, false)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer m.Close()

	// The container opens stdin non-blocking (safe regardless of peers) and
	// then switches to blocking mode, matching runc's behaviour. The manager's
	// write gate is already held, so the open cannot stall.
	containerIn, err := os.OpenFile(m.StdinPath(), unix.O_RDONLY|unix.O_NONBLOCK, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer containerIn.Close()
	if flags, err := unix.FcntlInt(containerIn.Fd(), unix.F_GETFL, 0); err != nil {
		t.Fatal(err)
	} else if _, err := unix.FcntlInt(containerIn.Fd(), unix.F_SETFL, flags&^unix.O_NONBLOCK); err != nil {
		t.Fatalf("set stdin blocking: %v", err)
	}

	// Empty stdin blocks the container: run the read in a goroutine and prove
	// it does not return until data is written.
	readCh := make(chan readOutcome, 1)
	go func() {
		b := make([]byte, 16)
		n, err := containerIn.Read(b)
		readCh <- readOutcome{n: n, data: b[:n], err: err}
	}()
	select {
	case o := <-readCh:
		t.Fatalf("empty stdin read returned early: %q %v", o.data, o.err)
	case <-time.After(50 * time.Millisecond):
	}

	if _, err := m.WriteStdin([]byte("input-line")); err != nil {
		t.Fatalf("WriteStdin: %v", err)
	}
	select {
	case o := <-readCh:
		if o.err != nil || string(o.data) != "input-line" {
			t.Fatalf("expected input-line, got %q %v", o.data, o.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("container did not receive stdin data")
	}

	// After CloseStdin a fresh read must return EOF, proving no residual
	// management-side reader keeps the pipe open.
	if err := m.CloseStdin(); err != nil {
		t.Fatalf("CloseStdin: %v", err)
	}
	eofCh := make(chan error, 1)
	go func() {
		_, err := containerIn.Read(make([]byte, 1))
		eofCh <- err
	}()
	select {
	case err := <-eofCh:
		if err != io.EOF {
			t.Fatalf("expected stdin EOF, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("container did not observe stdin EOF after CloseStdin")
	}
}

// readOutcome carries one container-side read result.
type readOutcome struct {
	n    int
	data []byte
	err  error
}

// TestFifoWriteThenReadAndReadThenWrite proves data flow works against a real
// fifo regardless of whether the writer or the relay-side reader opens first,
// as long as the management read end (always held in production) is present.
// Data written before the relay drains is buffered and read intact.
func TestFifoWriteThenReadAndReadThenWrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "stream")
	if err := EnsureFIFO(path, os.Getuid(), os.Getgid()); err != nil {
		t.Fatal(err)
	}

	// Order 1: the management read end is held, a writer writes, then another
	// reader drains the buffered data intact.
	r1, err := openReadEnd(path)
	if err != nil {
		t.Fatalf("open read end: %v", err)
	}
	w1, err := openWriteEnd(path)
	if err != nil {
		t.Fatalf("open write end: %v", err)
	}
	if _, err := w1.Write([]byte("buffered-before-drain")); err != nil {
		t.Fatalf("write before drain: %v", err)
	}
	buf1 := make([]byte, len("buffered-before-drain"))
	if _, err := io.ReadFull(r1, buf1); err != nil {
		t.Fatalf("read buffered data: %v", err)
	}
	if string(buf1) != "buffered-before-drain" {
		t.Fatalf("got %q", buf1)
	}
	_ = w1.Close()
	_ = r1.Close()

	// Order 2: a fresh reader opens and waits, then the writer arrives.
	r2, err := openReadEnd(path)
	if err != nil {
		t.Fatalf("open read end first: %v", err)
	}
	w2, err := openWriteEnd(path)
	if err != nil {
		t.Fatalf("open write end after reader: %v", err)
	}
	if _, err := w2.Write([]byte("after-reader")); err != nil {
		t.Fatalf("write after reader: %v", err)
	}
	buf2 := make([]byte, len("after-reader"))
	if _, err := io.ReadFull(r2, buf2); err != nil {
		t.Fatalf("read after-reader data: %v", err)
	}
	if string(buf2) != "after-reader" {
		t.Fatalf("got %q", buf2)
	}
	_ = w2.Close()
	_ = r2.Close()
}

// TestOpenEndToEndBothOrders runs the full adapter with both fifo open orders
// (reader-first via the manager, writer-first container; and container opening
// before the relay attaches is covered by the non-blocking opens themselves),
// then verifies client disconnect survival, full-tail drain and truncation.
func TestOpenEndToEnd(t *testing.T) {
	dir := t.TempDir()
	stdoutSink := &recordingSink{}
	cfg := Config{
		ID:           "task-1",
		Dir:          dir,
		UID:          os.Getuid(),
		GID:          os.Getgid(),
		OpenStdin:    true,
		OpenStdout:   true,
		OpenStderr:   true,
		StdoutSink:   stdoutSink,
		DrainTimeout: 300 * time.Millisecond,
		Logger:       quietLogger(),
	}

	m, err := Open(cfg, false)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer m.Close()

	// The fifo directory and all three fifos exist with the documented
	// layout and ownership.
	for _, p := range []string{m.StdinPath(), m.StdoutPath(), m.StderrPath()} {
		isFIFO(t, p)
		info, _ := os.Lstat(p)
		if info.Sys().(*syscall.Stat_t).Uid != uint32(os.Getuid()) {
			t.Fatalf("%s not chowned", p)
		}
	}

	// Container side: use the same deadlock-free open ordering as the
	// adapter. A write end opened O_WRONLY|O_NONBLOCK fails with ENXIO while
	// no reader exists, so a temporary read hold is kept for the open.
	openContainerWriter := func() (*os.File, *os.File) {
		hold, err := os.OpenFile(m.StdoutPath(), unix.O_RDONLY|unix.O_NONBLOCK, 0)
		if err != nil {
			t.Fatalf("temp read hold: %v", err)
		}
		w, err := os.OpenFile(m.StdoutPath(), unix.O_WRONLY|unix.O_NONBLOCK, 0)
		if err != nil {
			hold.Close()
			t.Fatalf("container writer open: %v", err)
		}
		return w, hold
	}
	containerIn, err := os.OpenFile(m.StdinPath(), unix.O_RDONLY|unix.O_NONBLOCK, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer containerIn.Close()
	containerOut, outHold := openContainerWriter()
	defer outHold.Close()
	defer containerOut.Close()

	// 1. A client attaches, receives data, then disconnects. The container
	// keeps writing without SIGPIPE/EPIPE and the tail lands in the sink.
	clientR, clientW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := m.AttachClient(StreamStdout, clientW); err != nil {
		t.Fatal(err)
	}
	if _, err := containerOut.Write([]byte("first line\n")); err != nil {
		t.Fatalf("container write: %v", err)
	}
	buf := make([]byte, len("first line\n"))
	if _, err := io.ReadFull(clientR, buf); err != nil {
		t.Fatalf("client read: %v", err)
	}
	clientR.Close()
	clientW.Close()
	if err := m.DetachClient(StreamStdout); err != nil {
		t.Fatal(err)
	}

	// 2. Stdin: data forwarded, EOF only after CloseStdin.
	if _, err := m.WriteStdin([]byte("hello stdin")); err != nil {
		t.Fatalf("WriteStdin: %v", err)
	}
	inBuf := make([]byte, len("hello stdin"))
	if _, err := io.ReadFull(containerIn, inBuf); err != nil {
		t.Fatalf("container stdin read: %v", err)
	}
	if string(inBuf) != "hello stdin" {
		t.Fatalf("got %q", inBuf)
	}

	// 3. Large burst then immediate exit: the last line must survive. All
	// writes and the writer close happen before the exit-driven drain so the
	// relay can consume the whole pipe up to the resulting EOF.
	const lastLine = "THE-LAST-LINE\n"
	big := make([]byte, 256*1024)
	for i := range big {
		big[i] = 'x'
	}
	if _, err := containerOut.Write(big); err != nil {
		t.Fatalf("big write: %v", err)
	}
	if _, err := containerOut.Write([]byte(lastLine)); err != nil {
		t.Fatalf("last line write: %v", err)
	}
	// Process exits and drops its write end: EOF becomes observable.
	if err := containerOut.Close(); err != nil {
		t.Fatal(err)
	}

	m.MarkExited()

	// EOF on stdin must have been delivered by the exit.
	eofBuf := make([]byte, 1)
	if n, err := containerIn.Read(eofBuf); err != io.EOF || n != 0 {
		t.Fatalf("stdin should be EOF after exit, n=%d err=%v", n, err)
	}

	results := m.Drain()
	var stdoutRes *DrainResult
	for i := range results {
		if results[i].Stream == StreamStdout {
			stdoutRes = &results[i]
		}
	}
	if stdoutRes == nil || !stdoutRes.EOF || stdoutRes.Truncated {
		t.Fatalf("stdout drain not clean: %+v", results)
	}
	// The relay also counted "first line\n" that went to the attached client
	// before it disconnected, so total consumed includes it while the sink
	// only holds the post-disconnect fail-over data.
	const firstLine = "first line\n"
	wantConsumed := len(firstLine) + len(big) + len(lastLine)
	if stdoutRes.Bytes != int64(wantConsumed) {
		t.Fatalf("consumed %d bytes, want %d", stdoutRes.Bytes, wantConsumed)
	}
	got := stdoutSink.String()
	wantSinkLen := len(big) + len(lastLine)
	if len(got) != wantSinkLen {
		t.Fatalf("sink holds %d bytes, want %d", len(got), wantSinkLen)
	}
	if got[len(got)-len(lastLine):] != lastLine {
		t.Fatalf("last line truncated: tail=%q", got[len(got)-40:])
	}
}

// TestOpenGrandchildHoldsWriteEndTruncates verifies the timeout/truncation
// contract end to end against a real fifo.
func TestOpenGrandchildHoldsWriteEndTruncates(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{
		ID:           "task-grandchild",
		Dir:          dir,
		UID:          os.Getuid(),
		GID:          os.Getgid(),
		OpenStdout:   true,
		DrainTimeout: 150 * time.Millisecond,
		Logger:       quietLogger(),
	}
	m, err := Open(cfg, false)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	hold, err := os.OpenFile(m.StdoutPath(), unix.O_RDONLY|unix.O_NONBLOCK, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer hold.Close()
	containerOut, err := os.OpenFile(m.StdoutPath(), unix.O_WRONLY|unix.O_NONBLOCK, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := containerOut.Write([]byte("partial\n")); err != nil {
		t.Fatal(err)
	}
	// Deliberately keep containerOut open across the drain: the grandchild.
	defer containerOut.Close()

	m.MarkExited()
	start := time.Now()
	results := m.Drain()
	elapsed := time.Since(start)

	var found bool
	for _, r := range results {
		if r.Stream == StreamStdout {
			found = true
			if !r.Truncated || r.EOF {
				t.Fatalf("expected truncation, got %+v", r)
			}
			if r.Bytes != int64(len("partial\n")) {
				t.Fatalf("already-written bytes must be consumed, got %d", r.Bytes)
			}
		}
	}
	if !found {
		t.Fatal("no stdout drain result")
	}
	if elapsed > time.Second {
		t.Fatalf("drain waited indefinitely: %v", elapsed)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestOpenReuseResetsFIFOs covers reuse=true: a fresh Open replaces old fifos.
func TestOpenReuseResetsFIFOs(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{
		ID:         "reuse-task",
		Dir:        dir,
		UID:        os.Getuid(),
		GID:        os.Getgid(),
		OpenStdout: true,
		Logger:     quietLogger(),
	}
	m1, err := Open(cfg, false)
	if err != nil {
		t.Fatal(err)
	}
	path := m1.StdoutPath()
	info1, _ := os.Lstat(path)
	if err := m1.Close(); err != nil {
		t.Fatal(err)
	}

	m2, err := Open(cfg, true)
	if err != nil {
		t.Fatal(err)
	}
	defer m2.Close()
	info2, _ := os.Lstat(m2.StdoutPath())
	if os.SameFile(info1, info2) {
		t.Fatal("reuse Open must reset the fifo to a fresh inode")
	}
}

// TestRemoveFIFOs cleans up the created paths on task delete.
func TestRemoveFIFOs(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{
		ID:         "cleanup-task",
		Dir:        dir,
		UID:        os.Getuid(),
		GID:        os.Getgid(),
		OpenStdin:  true,
		OpenStdout: true,
		OpenStderr: true,
		Logger:     quietLogger(),
	}
	m, err := Open(cfg, false)
	if err != nil {
		t.Fatal(err)
	}
	paths := []string{m.StdinPath(), m.StdoutPath(), m.StderrPath()}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	if err := RemoveFIFOs(cfg); err != nil {
		t.Fatal(err)
	}
	for _, p := range paths {
		if _, err := os.Lstat(p); !os.IsNotExist(err) {
			t.Fatalf("fifo %s not removed", p)
		}
	}
	// Removing again is not an error (idempotent cleanup).
	if err := RemoveFIFOs(cfg); err != nil {
		t.Fatalf("second RemoveFIFOs: %v", err)
	}
}
