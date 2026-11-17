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
	"os"
	"path/filepath"
	"testing"
)

// TestAdapterSupport is cross-platform. On Linux the adapter works; elsewhere
// it must report ErrUnsupported so callers can skip only the real-fifo layer
// while the pipe-based lifecycle tests keep running.
func TestAdapterSupport(t *testing.T) {
	path := filepath.Join(t.TempDir(), "probe")
	err := EnsureFIFO(path, os.Getuid(), os.Getgid())
	if supportedPlatform() {
		if err != nil {
			t.Fatalf("EnsureFIFO failed on supported platform: %v", err)
		}
		return
	}
	if !errors.Is(err, ErrUnsupported) {
		t.Fatalf("unsupported platform must return ErrUnsupported, got %v", err)
	}
	if _, err := Open(Config{}, false); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("Open must return ErrUnsupported, got %v", err)
	}
	if err := ResetFIFO("unused-path", 0, 0); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("ResetFIFO must return ErrUnsupported, got %v", err)
	}
	if err := RemoveFIFOs(Config{}); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("RemoveFIFOs must return ErrUnsupported, got %v", err)
	}
}
