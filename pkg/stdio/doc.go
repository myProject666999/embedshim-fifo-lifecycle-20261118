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

// Package stdio manages the lifecycle of the named pipes (fifos) that carry a
// container process's stdin/stdout/stderr.
//
// # Three independent lifecycles
//
// A container's stdio involves three objects whose lifetimes must NOT be tied
// together:
//
//  1. The fifo itself. It is created before the container process starts and
//     outlives every individual client connection.
//  2. The log copy (the relay). A goroutine permanently drains the read end of
//     an output fifo and forwards the data. After the process exits it keeps
//     running in order to drain the tail; it only stops on EOF or on a bounded
//     timeout.
//  3. The client connection (the downstream). A client (for example a
//     `ctr task attach` session) may attach and detach any number of times.
//     Detaching only removes a downstream; it never closes the fifo.
//
// The rest of this comment records the design decisions. Each decision is
// exercised by the package tests.
//
// # FIFO creation contract
//
// Fifos are created before the container is started, see Open and EnsureFIFO.
// The directory, permission bits and owner are chosen so that the container's
// I/O user (Config.UID/Config.GID) can open the path. Four reuse cases are
// defined explicitly:
//
//   - Parent directory does not exist: it is created (MkdirAll), so an already
//     existing directory is accepted idempotently.
//   - The path does not exist: the fifo is created with Mkfifo.
//   - The path already exists and is a fifo: EnsureFIFO keeps the same inode
//     (open file descriptors stay valid) and only re-applies owner/mode. This
//     makes same-task retries idempotent.
//   - The path exists as a regular file (or any non-fifo, non-symlink entry):
//     it is removed and replaced by a fresh fifo, because a regular file at a
//     fifo path can never be opened correctly by the runtime.
//   - A symlink is rejected (ErrSymlinkNotAllowed). The fifo directory is
//     shim-owned state and following an untrusted link must not be possible.
//
// Reuse by a NEW task is a different operation: Open calls ResetFIFO, which
// unlinks any stale entry (old fifo or stray regular file) and creates a clean
// inode, so a dead task cannot leave a pipe behind that still has peers or
// stale data.
//
// Long container IDs are handled by FifoPath: when a path component would
// exceed NAME_MAX or the whole path would get close to PATH_MAX, the ID
// component is replaced by a deterministic SHA-256 hash, which is stable for
// the same ID and always length-bounded.
//
// # Open ordering: never depend on who opens first
//
// A blocking open(2) on a fifo deadlocks if the peer never shows up:
// O_RDONLY blocks until a writer exists and O_WRONLY blocks until a reader
// exists (O_WRONLY|O_NONBLOCK even fails with ENXIO). The package therefore
// never uses blocking opens while waiting for a peer:
//
//   - The management side opens its permanent read end with
//     O_RDONLY|O_NONBLOCK, which on Linux succeeds even with no writer. The fd
//     is deliberately left non-blocking and is driven with poll(2) together
//     with an abort pipe, so a blocked read can be cancelled when the drain
//     timeout fires (a blocking read cannot be cancelled: closing the fd it is
//     parked on would itself block on Go's poller lock).
//   - A write end is opened with O_WRONLY|O_NONBLOCK only while a temporary
//     O_RDONLY|O_NONBLOCK fd is held, so ENXIO is impossible; the temporary
//     fd is closed immediately afterwards. Keeping it would prevent the
//     container-side reader from seeing EOF once the stdin gate is closed.
//
// Read-hold versus write-hold: an extra WRITE end keeps the pipe open, so the
// read side can never observe EOF. An extra READ end does not have that
// effect. The package therefore keeps a permanent READ hold on every output
// fifo for the whole lifetime, and never keeps a write hold on an output
// fifo. The only permanent write hold is the stdin gate (see below). If an
// implementation ever needs an O_RDWR hold end to avoid an open deadlock, the
// write side of that hold MUST be closed before draining starts; Manager
// implements that ordering in MarkExited.
//
// # Client disconnect must never break the container
//
// Two approaches were considered.
//
// Approach A - permanent reader plus a relay (CHOSEN). The management side
// always owns a read end. The relay reads from it continuously and writes each
// chunk to the currently attached downstream. With no downstream, or when the
// downstream write fails because the client went away, the chunk is routed to
// the fallback sink (a log file or io.Discard). Because the read end is always
// present, the container's write(2) can never get EPIPE/SIGPIPE, no matter how
// many clients connect and disconnect.
//
// Approach B - let the pipe break and hope the process ignores SIGPIPE.
// Rejected: the result depends on each application's signal disposition, a
// default SIGPIPE kills the process, delivery ordering is undefined, and tail
// data written around the disconnect may be silently lost. It cannot be made
// deterministic.
//
// A stuck or dead downstream can never block the relay: a failed or partial
// downstream write detaches the downstream and the remaining bytes go to the
// sink.
//
// # Exit notification and draining are independent
//
// A process-exit event does not stop the copy. On MarkExited the relays enter
// draining mode and keep consuming until either:
//
//   - EOF is observed on the read end (every writer, including the
//     container's, was closed): the drain completes cleanly and
//     DrainResult.Truncated is false; or
//   - the bounded DrainTimeout expires: Unblock signals the abort pipe to
//     release the blocked read (which returns an abort error, not an EOF), and
//     the drain ends with DrainResult.Truncated=true and a reason string. The
//     event is logged.
//
// Waiting forever is explicitly NOT supported: after the main process exits a
// grandchild may legitimately inherit and keep the write end open, in which
// case EOF would never arrive. An unbounded wait would leak the goroutine and
// the fds and block task Delete indefinitely. The bounded timeout makes the
// behavior deterministic: common cases keep their full tail (EOF usually
// arrives within milliseconds), while a held write end is reported as
// truncation instead of hanging.
//
// # Input direction
//
// The management side keeps a permanent writer on the stdin fifo (the stdin
// gate). While that gate is open and no client is sending data, the
// container's read(2) blocks: an absent client means "no input yet", not EOF.
// EOF is delivered only on an explicit CloseStdin (the runtime CloseIO call)
// or when the process exits and MarkExited closes the gate.
//
// # Terminal mode
//
// With Config.Terminal set, stdout and stderr are one merged stream: a single
// fifo and a single relay are created and the stderr path stays empty. In
// non-terminal mode stdout and stderr each get their own fifo, relay and sink.
//
// # Platform support
//
// The lifecycle, relay and drain logic only depends on the Source, Sink and
// io.Writer/io.Closer abstractions and is fully covered on every platform with
// pipe-based fakes. Only the fifo adapter is Linux specific: on platforms
// without fifo support Open/EnsureFIFO/ResetFIFO return ErrUnsupported and the
// real-fifo tests are not compiled at all (build-tagged), so the cross-platform
// lifecycle/drain tests still run everywhere.
package stdio
