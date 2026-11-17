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
	"log"
	"os"
	"sync"
	"time"
)

// Stream names. In terminal mode stderr is merged into stdout and
// StreamStderr is never used.
const (
	StreamStdin  = "stdin"
	StreamStdout = "stdout"
	StreamStderr = "stderr"
)

// DefaultDrainTimeout bounds the post-exit drain. EOF normally arrives within
// milliseconds; this only fires when a grandchild keeps the write end open.
const DefaultDrainTimeout = 30 * time.Second

// Config describes the stdio of one task (an init process or an exec process).
type Config struct {
	// ID is the task/process identifier used in fifo path construction.
	ID string
	// Dir is the root directory that holds the per-task fifo directory.
	Dir string
	// UID/GID own the fifos so that the container's I/O user can open them.
	UID int
	GID int
	// Terminal merges stderr into stdout and skips the stderr fifo/relay.
	Terminal bool
	// OpenStdin/OpenStdout/OpenStderr select which streams exist.
	OpenStdin  bool
	OpenStdout bool
	OpenStderr bool
	// Sinks receive output when no client is attached or when a client fails
	// a write. A nil entry falls back to DiscardSink. In terminal mode only
	// StdoutSink is used.
	StdoutSink Sink
	StderrSink Sink
	// DrainTimeout bounds draining after MarkExited. Zero means
	// DefaultDrainTimeout; a negative value means wait for EOF without a
	// timeout (mainly useful in tests).
	DrainTimeout time.Duration
	// Logger receives operational messages. nil uses the standard logger.
	Logger *log.Logger
}

// stream bundles one logical fifo with its relay and, for output streams, the
// permanent management-side read end.
type stream struct {
	name string
	path string

	// source is the permanent read end (stdout/stderr only).
	source Source
	// relay is the copy loop (stdout/stderr only).
	relay *relay

	// stdinWriter is the stdin gate: while open the container blocks on
	// empty stdin; closing it delivers EOF. stdin only.
	stdinWriter io.WriteCloser
}

// Manager owns the fifos, relays and the stdin gate for one task. The zero
// value is not usable; obtain one from Open or newManagerWithStreams.
type Manager struct {
	cfg     Config
	logger  *log.Logger
	streams map[string]*stream

	mu       sync.Mutex
	exited   bool
	closed   bool
	drained  bool
	drainRes []DrainResult
	wg       sync.WaitGroup
}

func newManager(cfg Config) *Manager {
	logger := cfg.Logger
	if logger == nil {
		logger = log.Default()
	}
	if cfg.DrainTimeout == 0 {
		cfg.DrainTimeout = DefaultDrainTimeout
	}
	return &Manager{
		cfg:     cfg,
		logger:  logger,
		streams: make(map[string]*stream),
	}
}

// newManagerWithStreams builds a Manager from already-opened streams. It is the
// platform-independent core used by the Linux adapter and by tests: the stream
// endpoints can be real fifo fds or pipe-based fakes, so every lifecycle and
// drain semantic is testable without fifos.
func newManagerWithStreams(cfg Config, ss ...*stream) *Manager {
	m := newManager(cfg)
	for _, s := range ss {
		if s == nil {
			continue
		}
		m.streams[s.name] = s
		if s.relay != nil {
			m.wg.Add(1)
			go func(r *relay) {
				defer m.wg.Done()
				r.wait()
			}(s.relay)
		}
	}
	return m
}

// StdinPath/StdoutPath/StderrPath expose the fifo paths handed to the runtime.
func (m *Manager) StdinPath() string {
	if s, ok := m.streams[StreamStdin]; ok {
		return s.path
	}
	return ""
}

func (m *Manager) StdoutPath() string {
	if s, ok := m.streams[StreamStdout]; ok {
		return s.path
	}
	return ""
}

func (m *Manager) StderrPath() string {
	if s, ok := m.streams[StreamStderr]; ok {
		return s.path
	}
	return ""
}

// AttachClient registers w as the current downstream for one output stream.
// Passing nil detaches the client. Only one downstream per stream exists at a
// time; attaching replaces the previous one and never touches the fifo, so a
// disconnect here cannot propagate EPIPE/SIGPIPE to the container.
func (m *Manager) AttachClient(name string, w io.Writer) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return errors.New("stdio: manager already closed")
	}
	s, ok := m.streams[name]
	if !ok || s.relay == nil {
		return errors.New("stdio: no output stream named " + name)
	}
	s.relay.setDownstream(w)
	return nil
}

// DetachClient removes the downstream without closing anything.
func (m *Manager) DetachClient(name string) error {
	return m.AttachClient(name, nil)
}

// WriteStdin forwards p into the stdin fifo. It returns an error once the
// stdin gate has been closed (CloseStdin or process exit).
func (m *Manager) WriteStdin(p []byte) (int, error) {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return 0, errors.New("stdio: manager already closed")
	}
	s, ok := m.streams[StreamStdin]
	m.mu.Unlock()
	if !ok || s.stdinWriter == nil {
		return 0, errors.New("stdio: stdin is not open")
	}
	return s.stdinWriter.Write(p)
}

// CloseStdin closes the stdin gate, delivering EOF to the container's next
// read(2). Before that point an absent client means blocking, not EOF.
func (m *Manager) CloseStdin() error {
	m.mu.Lock()
	s, ok := m.streams[StreamStdin]
	m.mu.Unlock()
	if !ok || s.stdinWriter == nil {
		return nil
	}
	return s.stdinWriter.Close()
}

// MarkExited records that the process has exited and starts the draining
// phase. Crucially it does NOT stop any relay. It also closes the stdin gate
// first: any extra write hold must be released before draining, otherwise the
// read side could never observe EOF.
func (m *Manager) MarkExited() {
	m.mu.Lock()
	if m.exited || m.closed {
		m.mu.Unlock()
		return
	}
	m.exited = true

	if s, ok := m.streams[StreamStdin]; ok && s.stdinWriter != nil {
		_ = s.stdinWriter.Close()
	}
	for _, s := range m.streams {
		if s.relay != nil {
			s.relay.markExited()
		}
	}
	m.mu.Unlock()
}

// Drain blocks until every relay has consumed the post-exit tail, bounded by
// Config.DrainTimeout. It is safe to call after MarkExited; calling it before
// an exit has no effect and returns nil. Results are also returned by Close.
func (m *Manager) Drain() []DrainResult {
	m.mu.Lock()
	if m.drained {
		res := m.drainRes
		m.mu.Unlock()
		return res
	}
	if !m.exited {
		m.mu.Unlock()
		return nil
	}
	m.drained = true
	relays := make([]*relay, 0, len(m.streams))
	for _, s := range m.streams {
		if s.relay != nil {
			relays = append(relays, s.relay)
		}
	}
	timeout := m.cfg.DrainTimeout
	m.mu.Unlock()

	results := make([]DrainResult, 0, len(relays))
	for _, r := range relays {
		res := r.drain(timeout)
		results = append(results, res)
		if res.Truncated {
			m.logger.Printf("stdio: %s output truncated: %s (bytes consumed: %d)",
				res.Stream, res.Reason, res.Bytes)
		}
	}

	m.mu.Lock()
	m.drainRes = results
	m.mu.Unlock()
	return results
}

// Close releases every endpoint and sink. It drains first when the process has
// already exited, so Close is safe to call directly from task Delete. When it
// closes before an exit it aborts the relays instead of leaving reads blocked.
// It is idempotent.
func (m *Manager) Close() error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.closed = true
	needDrain := m.exited && !m.drained
	relays := make([]*relay, 0, len(m.streams))
	for _, s := range m.streams {
		if s.relay != nil {
			relays = append(relays, s.relay)
		}
	}
	m.mu.Unlock()

	if needDrain {
		m.Drain()
	} else {
		// No post-exit drain was requested: release blocked reads so the
		// relay goroutines can terminate instead of leaking on the fifo.
		for _, r := range relays {
			r.abort()
		}
	}

	var err error
	for _, s := range m.streams {
		// stdin: release the gate so a blocked container-side read gets EOF.
		if s.stdinWriter != nil {
			if cerr := s.stdinWriter.Close(); cerr != nil && !errors.Is(cerr, os.ErrClosed) {
				err = cerr
			}
		}
		// output relays own their sources; wait for them so the fd close is
		// ordered after the last read.
		if s.relay != nil {
			<-s.relay.doneCh()
		}
	}
	m.wg.Wait()

	for _, s := range m.streams {
		if s.relay != nil {
			if cerr := s.relay.sink.Close(); cerr != nil && !errors.Is(cerr, io.ErrClosedPipe) {
				if err == nil {
					err = cerr
				}
			}
		}
	}
	return err
}

// Exited reports whether MarkExited has been called.
func (m *Manager) Exited() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.exited
}
