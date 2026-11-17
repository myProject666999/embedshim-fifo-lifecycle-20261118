//go:build !linux
// +build !linux

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

// On platforms without named-pipe support the adapter is unavailable. The
// platform-independent lifecycle/drain logic in manager.go and relay.go still
// builds and is covered by pipe-based tests; only real-fifo tests are skipped.

// supportedPlatform reports whether the real-fifo adapter is available.
func supportedPlatform() bool { return false }

// EnsureFIFO returns ErrUnsupported off Linux.
func EnsureFIFO(path string, uid, gid int) error { return ErrUnsupported }

// ResetFIFO returns ErrUnsupported off Linux.
func ResetFIFO(path string, uid, gid int) error { return ErrUnsupported }

// Open returns ErrUnsupported off Linux.
func Open(cfg Config, reuse bool) (*Manager, error) { return nil, ErrUnsupported }

// RemoveFIFOs returns ErrUnsupported off Linux.
func RemoveFIFOs(cfg Config) error { return ErrUnsupported }
