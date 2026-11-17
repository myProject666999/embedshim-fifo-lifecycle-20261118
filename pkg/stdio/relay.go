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
	"errors"
	"io"
	"log"
	"os"
	"sync"
	"time"
)

// Source is the permanent management-side read end of an output stream
// (stdout/stderr). The relay owns it and closes it on teardown or on a drain
// timeout.
//
// A blocking read cannot be cancelled by closing the same fd the read is
// parked on (Go's os.File.Close takes the lock an in-flight Read already
// holds). Sources that need cancellation therefore also implement unblocker:
// Unblock makes a blocked Read return errDrainAborted through a separate
// mechanism (the Linux adapter polls the fifo together with an abort pipe).
type Source interface {
	io.Reader
	io.Closer
}

// unblocker is implemented by sources that can release a blocked Read without
// closing the read fd. When available, drain calls Unblock instead of Close
// on timeout; the relay then closes the read fd once the read has returned.
type unblocker interface {
	Unblock() error
}

// errDrainAborted is returned by a Source.Read when Unblock released it
// because the drain timed out. Unlike io.EOF it means "the tail may be
// incomplete", so the relay records a truncation rather than a clean finish.
var errDrainAborted = errors.New("stdio: read aborted by drain timeout")

// Sink receives output while no client is attached, or when the attached
// client cannot accept the data. It is the log-file / discard fallback that
// guarantees the container never blocks because a reader went away.
type Sink interface {
	io.Writer
	io.Closer
}

// discardSink routes data to io.Discard without an error. It is the default
// fallback when logging is disabled. The relay's own byte counter already
// accounts for every byte it reads, so the sink stays stateless.
type discardSink struct{}

func (discardSink) Write(p []byte) (int, error) { return len(p), nil }
func (discardSink) Close() error                { return nil }

// DiscardSink is the Sink that drops every byte without an error.
var DiscardSink Sink = discardSink{}

// relay permanently copies data from a Source, first to the attached
// downstream and otherwise to the fallback Sink. Its lifetime is independent
// of both the fifo and the client connection.
type relay struct {
	name   string
	source Source
	sink   Sink
	logger *log.Logger

	// downstreamMu guards downstream. A nil downstream means "no client":
	// every chunk goes straight to the sink.
	downstreamMu sync.Mutex
	downstream   io.Writer

	// drainCh is closed once the process has exited; after that point a read
	// returning zero data with no error (only possible on a fifo with no
	// writer between reads) is treated as EOF instead of being retried.
	drainCh chan struct{}
	done    chan struct{}

	// DrainResult is only meaningful after done is closed.
	resultMu sync.Mutex
	result   DrainResult
}

// DrainResult reports how a relay ended.
type DrainResult struct {
	// Stream is the logical stream name ("stdout" or "stderr").
	Stream string
	// Bytes is the total number of bytes read from the source.
	Bytes int64
	// EOF is true when the relay stopped because every write end was closed.
	EOF bool
	// Truncated is true when the drain timed out with data potentially still
	// in the pipe. EOF and Truncated are mutually exclusive.
	Truncated bool
	// Reason explains a truncated drain and is empty otherwise.
	Reason string
}

func newRelay(name string, source Source, sink Sink, logger *log.Logger) *relay {
	if sink == nil {
		sink = DiscardSink
	}
	if logger == nil {
		logger = log.Default()
	}
	r := &relay{
		name:    name,
		source:  source,
		sink:    sink,
		logger:  logger,
		drainCh: make(chan struct{}),
		done:    make(chan struct{}),
	}
	go r.run()
	return r
}

// setDownstream replaces the current client. Passing nil detaches the client;
// existing in-flight writes finish (or fail over to the sink) regardless.
func (r *relay) setDownstream(w io.Writer) {
	r.downstreamMu.Lock()
	r.downstream = w
	r.downstreamMu.Unlock()
}

func (r *relay) getDownstream() io.Writer {
	r.downstreamMu.Lock()
	w := r.downstream
	r.downstreamMu.Unlock()
	return w
}

// markExited switches the relay into draining mode. It does not stop anything:
// the loop keeps reading until EOF or drain returns.
func (r *relay) markExited() {
	select {
	case <-r.drainCh:
	default:
		close(r.drainCh)
	}
}

// abort stops the relay immediately, regardless of state. It is used when the
// Manager closes without a post-exit drain: the held sources must still be
// released and blocked reads must be cancelled. The result is truncated rather
// than EOF because the tail was intentionally not drained.
func (r *relay) abort() {
	r.markExited()
	if ub, ok := r.source.(unblocker); ok {
		_ = ub.Unblock()
	} else {
		_ = r.source.Close()
	}
}

func (r *relay) draining() bool {
	select {
	case <-r.drainCh:
		return true
	default:
		return false
	}
}

// wait blocks until the relay has stopped and returns its drain result.
func (r *relay) wait() DrainResult {
	<-r.done
	r.resultMu.Lock()
	res := r.result
	r.resultMu.Unlock()
	return res
}

// done returns the channel closed when the relay stops.
func (r *relay) doneCh() <-chan struct{} { return r.done }

// run is the copy loop. It exits only on EOF, an unrecoverable read error, or
// when drain closes the source after the timeout.
func (r *relay) run() {
	var (
		total     int64
		buf       = make([]byte, 4096) // aligns with PIPE_BUF, see pipe(7)
		eof       bool
		truncated bool
		reason    string
	)

	defer func() {
		// The read goroutine is exiting, so closing the read fd cannot
		// deadlock with an in-flight Read here. The sink stays open until
		// Manager.Close so buffered tail data is not lost on the fallback
		// side.
		if cerr := r.source.Close(); cerr != nil && !errors.Is(cerr, os.ErrClosed) {
			r.logger.Printf("%s: closing source: %v", r.name, cerr)
		}
		r.resultMu.Lock()
		r.result = DrainResult{
			Stream:    r.name,
			Bytes:     total,
			EOF:       eof,
			Truncated: truncated,
			Reason:    reason,
		}
		r.resultMu.Unlock()
		close(r.done)
	}()

	for {
		n, err := r.source.Read(buf)
		if n > 0 {
			total += int64(n)
			r.forward(buf[:n])
		}

		if err != nil {
			if errors.Is(err, io.EOF) {
				eof = true
				return
			}
			if errors.Is(err, os.ErrClosed) || errors.Is(err, errDrainAborted) {
				// The source was released on purpose (drain timeout or
				// manager close): not a clean EOF, mark the tail truncated.
				truncated = true
				reason = "read aborted before EOF: " + err.Error()
				return
			}
			// Any other read error is terminal; record it like a truncation
			// so the caller knows the tail may be incomplete.
			truncated = true
			reason = "read error: " + err.Error()
			r.logger.Printf("%s: drain ended on read error: %v", r.name, err)
			return
		}

		// A zero-length read with no error is legal on a non-blocking fifo
		// while no writer is connected. Retry while the process is alive; in
		// draining mode it means the last writer is gone, i.e. EOF.
		if n == 0 && r.draining() {
			eof = true
			return
		}
	}
}

// forward writes one chunk to the downstream if a client is attached, and to
// the sink otherwise. A downstream that errors or accepts only part of the
// chunk is detached and the remaining bytes are guaranteed to reach the sink,
// so a slow/dead client can never block the copy or the container.
func (r *relay) forward(chunk []byte) {
	d := r.getDownstream()
	if d == nil {
		r.writeSink(chunk)
		return
	}

	written, err := d.Write(chunk)
	if err == nil && written == len(chunk) {
		return
	}

	// The client is gone or stuck. Detach it and fail over the remainder.
	r.setDownstream(nil)
	if written > 0 {
		chunk = chunk[written:]
	}
	r.logger.Printf("%s: downstream detached (err=%v); %d byte(s) fail over to sink",
		r.name, err, len(chunk))
	r.writeSink(chunk)
}

func (r *relay) writeSink(chunk []byte) {
	// io.Copy semantics: keep going on short writes.
	if _, err := io.Copy(r.sink, bytes.NewReader(chunk)); err != nil {
		r.logger.Printf("%s: writing to fallback sink: %v", r.name, err)
	}
}

// drain waits for the relay to finish after the process exited, bounded by
// timeout. On timeout the source is closed to unblock the pending read and the
// result is marked truncated. A non-positive timeout means "wait for EOF" and
// is only intended for tests of the clean path.
func (r *relay) drain(timeout time.Duration) DrainResult {
	if timeout <= 0 {
		return r.wait()
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-r.done:
	case <-timer.C:
		// A grandchild can inherit the write end and keep the fifo open
		// forever. Do not wait indefinitely: release the blocked read and
		// record the truncation. Unblock uses a separate control fd because
		// closing the exact fd the read is parked on would itself block.
		r.logger.Printf("%s: drain timed out after %s; releasing read end, remaining data is truncated",
			r.name, timeout)
		if ub, ok := r.source.(unblocker); ok {
			_ = ub.Unblock()
		} else {
			_ = r.source.Close()
		}
		<-r.done

		res := r.wait()
		if !res.EOF {
			res.Truncated = true
			res.Reason = "drain timeout after " + timeout.String() +
				": a process still holds the write end"
		}
		r.resultMu.Lock()
		r.result = res
		r.resultMu.Unlock()
		return res
	}
	return r.wait()
}
