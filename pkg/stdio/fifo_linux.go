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
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"

	"golang.org/x/sys/unix"
)

// fifoMode is the default permission of a fresh fifo. Only the owner (set to
// the container's I/O user via Fchown/Lchown below) needs access.
const fifoMode os.FileMode = 0600

// supportedPlatform reports whether the real-fifo adapter is available.
func supportedPlatform() bool { return true }

// EnsureFIFO creates the fifo at path idempotently and makes it usable by the
// given uid/gid. The handling of every existing-path case is defined:
//
//   - missing parent directory: created;
//   - missing path: fifo created;
//   - existing fifo: kept (same inode), owner/mode re-applied;
//   - existing regular/non-fifo file: removed and replaced;
//   - symlink: rejected with ErrSymlinkNotAllowed.
func EnsureFIFO(path string, uid, gid int) error {
	parent := filepath.Dir(path)
	if err := os.MkdirAll(parent, 0711); err != nil {
		return fmt.Errorf("stdio: create fifo directory %s: %w", parent, err)
	}

	// Lstat so a symlink is never followed.
	info, err := os.Lstat(path)
	switch {
	case err == nil:
		switch {
		case info.Mode()&os.ModeSymlink != 0:
			return fmt.Errorf("%w: %s", ErrSymlinkNotAllowed, path)
		case info.Mode()&os.ModeNamedPipe != 0:
			// Same task retrying: keep the inode, existing open fds stay
			// valid; just make sure ownership/permission are correct.
			return applyOwnership(path, uid, gid, true)
		default:
			// A regular file (socket, device, ...) cannot serve as a fifo.
			if err := os.Remove(path); err != nil {
				return fmt.Errorf("stdio: replace non-fifo entry %s: %w", path, err)
			}
		}
	case os.IsNotExist(err):
		// fall through and create it
	default:
		return fmt.Errorf("stdio: stat fifo %s: %w", path, err)
	}

	if err := unix.Mkfifo(path, uint32(fifoMode)); err != nil {
		return fmt.Errorf("stdio: mkfifo %s: %w", path, err)
	}
	return applyOwnership(path, uid, gid, false)
}

// applyOwnership sets uid/gid and mode. For a brand-new fifo fchowning an fd
// is preferable; for an already-existing fifo other fds may be open and the
// path must not be replaced, so lchown/chmod by path is used. Errors from
// chown (for example an unprivileged caller that cannot change owner) are
// returned so callers can fail loudly instead of handing the runtime an
// unopenable pipe.
func applyOwnership(path string, uid, gid int, existing bool) error {
	if existing {
		if err := os.Chmod(path, fifoMode); err != nil {
			return fmt.Errorf("stdio: chmod fifo %s: %w", path, err)
		}
		if err := os.Lchown(path, uid, gid); err != nil {
			return fmt.Errorf("stdio: chown fifo %s: %w", path, err)
		}
		return nil
	}

	f, err := os.OpenFile(path, unix.O_RDONLY|unix.O_NONBLOCK, 0)
	if err != nil {
		return fmt.Errorf("stdio: open new fifo %s for chown: %w", path, err)
	}
	defer f.Close()
	if err := unix.Fchown(int(f.Fd()), uid, gid); err != nil {
		return fmt.Errorf("stdio: fchown fifo %s: %w", path, err)
	}
	if err := unix.Fchmod(int(f.Fd()), uint32(fifoMode)); err != nil {
		return fmt.Errorf("stdio: fchmod fifo %s: %w", path, err)
	}
	return nil
}

// ResetFIFO unlinks any stale entry at path and creates a clean fifo. It is
// used when a NEW task reuses a path that belonged to a previous task, so old
// peers and buffered data cannot survive into the new task.
func ResetFIFO(path string, uid, gid int) error {
	info, err := os.Lstat(path)
	if err == nil {
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("%w: %s", ErrSymlinkNotAllowed, path)
		}
		if err := os.Remove(path); err != nil {
			return fmt.Errorf("stdio: remove stale fifo %s: %w", path, err)
		}
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("stdio: stat stale fifo %s: %w", path, err)
	}
	return EnsureFIFO(path, uid, gid)
}

// pollInterval bounds a single poll(2) call so Read periodically rechecks the
// aborted flag even if no fd becomes ready.
const pollInterval = 1000

// fifoReadEnd is the permanent management-side read end of an output fifo.
//
// The fifo fd is opened O_NONBLOCK and driven with poll(2) together with an
// abort pipe instead of a plain blocking read(2). A blocking read cannot be
// cancelled: Go's os.File.Close takes the poller lock an in-flight Read
// already holds, and closing a dup'ed fd does not wake a read on another fd
// of the same open file description. The abort pipe gives Unblock a reliable
// way to release a parked read with errDrainAborted, which the drain logic
// distinguishes from a genuine EOF.
//
// All fds are raw unix fds (never wrapped in *os.File) so the Go runtime
// netpoller never manages them and cannot interfere with the manual poll.
type fifoReadEnd struct {
	fd      int // non-blocking fifo read end
	abortR  int // read end of the abort pipe
	abortW  int // write end of the abort pipe, closed by Unblock
	mu      sync.Mutex
	aborted bool
	closed  bool
}

// Read blocks until data is available, every writer closes (io.EOF), or
// Unblock releases the wait (errDrainAborted).
func (f *fifoReadEnd) Read(b []byte) (int, error) {
	for {
		f.mu.Lock()
		if f.aborted {
			f.mu.Unlock()
			return 0, errDrainAborted
		}
		f.mu.Unlock()

		n, err := unix.Poll([]unix.PollFd{
			{Fd: int32(f.fd), Events: unix.POLLIN},
			{Fd: int32(f.abortR), Events: unix.POLLIN},
		}, pollInterval)
		if err == unix.EINTR {
			continue
		}
		if err != nil {
			return 0, fmt.Errorf("stdio: poll fifo: %w", err)
		}
		if n == 0 {
			continue // timeout: re-check the aborted flag
		}

		f.mu.Lock()
		aborted := f.aborted
		f.mu.Unlock()
		if aborted {
			return 0, errDrainAborted
		}

		nr, err := unix.Read(f.fd, b)
		if err == unix.EAGAIN || err == unix.EWOULDBLOCK {
			continue // a spurious readiness notification
		}
		if err != nil {
			return 0, fmt.Errorf("stdio: read fifo: %w", err)
		}
		if nr == 0 {
			// All write ends are closed: real end of stream.
			return 0, io.EOF
		}
		return nr, nil
	}
}

// Unblock releases any blocked Read without closing the fifo fd. It is
// idempotent.
func (f *fifoReadEnd) Unblock() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.aborted || f.closed {
		return nil
	}
	f.aborted = true
	if f.abortW >= 0 {
		// One byte wakes the poll; the pipe is non-blocking so this can
		// never stall.
		_, _ = unix.Write(f.abortW, []byte{1})
		_ = unix.Close(f.abortW)
		f.abortW = -1
	}
	return nil
}

// Close releases every fd. It first unblocks a parked read; the relay only
// calls Close from its cleanup, after the reading goroutine has exited.
func (f *fifoReadEnd) Close() error {
	_ = f.Unblock()
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return nil
	}
	f.closed = true
	fd, abortR, abortW := f.fd, f.abortR, f.abortW
	f.mu.Unlock()

	var err error
	if err1 := unix.Close(fd); err1 != nil && err == nil {
		err = err1
	}
	if abortR >= 0 {
		if err1 := unix.Close(abortR); err1 != nil && err == nil {
			err = err1
		}
	}
	if abortW >= 0 {
		_ = unix.Close(abortW)
	}
	return err
}

// openReadEnd opens the permanent management-side read end. O_RDONLY|O_NONBLOCK
// succeeds on Linux even when no writer exists, so the call never blocks and
// does not depend on the container opening first. The fd stays non-blocking
// because Read drives it with poll(2) plus the abort pipe.
func openReadEnd(path string) (*fifoReadEnd, error) {
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("stdio: open read end %s: %w", path, err)
	}

	var pipe [2]int
	if err := unix.Pipe2(pipe[:], unix.O_NONBLOCK|unix.O_CLOEXEC); err != nil {
		_ = unix.Close(fd)
		return nil, fmt.Errorf("stdio: create abort pipe for %s: %w", path, err)
	}
	return &fifoReadEnd{
		fd:     fd,
		abortR: pipe[0],
		abortW: pipe[1],
	}, nil
}

// openWriteEnd opens a write end without depending on a peer. O_WRONLY|O_NONBLOCK
// fails with ENXIO when no reader is present, so a temporary read hold is kept
// open for the duration of the open only and released immediately afterwards:
// keeping it would make the container-side reader miss EOF when the gate is
// later closed, because the hold would still count as a reader. The returned
// fd is blocking.
func openWriteEnd(path string) (*os.File, error) {
	readHold, err := os.OpenFile(path, unix.O_RDONLY|unix.O_NONBLOCK, 0)
	if err != nil {
		return nil, fmt.Errorf("stdio: open temporary read hold %s: %w", path, err)
	}
	defer readHold.Close()

	w, err := os.OpenFile(path, unix.O_WRONLY|unix.O_NONBLOCK, 0)
	if err != nil {
		return nil, fmt.Errorf("stdio: open write end %s: %w", path, err)
	}
	flags, err := unix.FcntlInt(w.Fd(), unix.F_GETFL, 0)
	if err != nil {
		_ = w.Close()
		return nil, fmt.Errorf("stdio: get flags for %s: %w", path, err)
	}
	if _, err := unix.FcntlInt(w.Fd(), unix.F_SETFL, flags&^unix.O_NONBLOCK); err != nil {
		_ = w.Close()
		return nil, fmt.Errorf("stdio: clear O_NONBLOCK on %s: %w", path, err)
	}
	return w, nil
}

// Open creates/opens every configured fifo and starts the relays. The fifo
// lifetimes are independent of the returned Manager's clients: callers keep
// the Manager for the whole task and attach/detach clients freely.
//
// When reuse is true every path is ResetFIFO'd first (fresh inode for a new
// task); otherwise EnsureFIFO's idempotent rules apply.
func Open(cfg Config, reuse bool) (_ *Manager, retErr error) {
	logger := cfg.Logger
	if logger == nil {
		logger = log.Default()
	}

	ensure := EnsureFIFO
	if reuse {
		ensure = ResetFIFO
	}

	var opened []*stream
	cleanup := func() {
		for _, s := range opened {
			if s.stdinWriter != nil {
				_ = s.stdinWriter.Close()
			}
			if s.source != nil {
				_ = s.source.Close()
			}
		}
	}
	defer func() {
		if retErr != nil {
			cleanup()
		}
	}()

	addOutput := func(name string, sink Sink) error {
		path, err := FifoPath(cfg.Dir, cfg.ID, name)
		if err != nil {
			return err
		}
		if err := ensure(path, cfg.UID, cfg.GID); err != nil {
			return err
		}
		// The read end is opened before any container-side writer can exist:
		// O_NONBLOCK makes that safe and guarantees the relay is consuming
		// before the process can produce data.
		src, err := openReadEnd(path)
		if err != nil {
			return err
		}
		s := &stream{
			name:   name,
			path:   path,
			source: src,
		}
		s.relay = newRelay(name, src, sink, logger)
		opened = append(opened, s)
		return nil
	}

	if cfg.OpenStdin {
		path, err := FifoPath(cfg.Dir, cfg.ID, StreamStdin)
		if err != nil {
			return nil, err
		}
		if err := ensure(path, cfg.UID, cfg.GID); err != nil {
			return nil, err
		}
		w, err := openWriteEnd(path)
		if err != nil {
			return nil, err
		}
		opened = append(opened, &stream{
			name:        StreamStdin,
			path:        path,
			stdinWriter: w,
		})
	}

	if cfg.OpenStdout {
		if err := addOutput(StreamStdout, cfg.StdoutSink); err != nil {
			return nil, err
		}
	}

	// Terminal mode: stderr shares the stdout stream; no separate fifo/relay.
	if cfg.OpenStderr && !cfg.Terminal {
		if err := addOutput(StreamStderr, cfg.StderrSink); err != nil {
			return nil, err
		}
	}

	return newManagerWithStreams(cfg, opened...), nil
}

// RemoveFIFOs unlinks the fifo paths that Open created. It is intended for task
// Delete after Manager.Close has released every fd.
func RemoveFIFOs(cfg Config) error {
	names := []string{}
	if cfg.OpenStdin {
		names = append(names, StreamStdin)
	}
	if cfg.OpenStdout {
		names = append(names, StreamStdout)
	}
	if cfg.OpenStderr && !cfg.Terminal {
		names = append(names, StreamStderr)
	}
	var firstErr error
	for _, name := range names {
		path, err := FifoPath(cfg.Dir, cfg.ID, name)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}
