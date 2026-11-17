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
	"strings"
	"testing"
)

func TestFifoPathLayout(t *testing.T) {
	p, err := FifoPath("/run/embedshim/io", "container-1", StreamStdout)
	if err != nil {
		t.Fatal(err)
	}
	if p != "/run/embedshim/io/container-1/stdout" {
		t.Fatalf("unexpected path %q", p)
	}
}

func TestFifoPathRejectsEmptyInput(t *testing.T) {
	if _, err := FifoPath("", "id", "stdout"); err == nil {
		t.Fatal("empty dir must error")
	}
	if _, err := FifoPath("/d", "", "stdout"); err == nil {
		t.Fatal("empty id must error")
	}
	if _, err := FifoPath("/d", "id", ""); err == nil {
		t.Fatal("empty stream must error")
	}
}

func TestFifoPathLongComponentIsHashed(t *testing.T) {
	longID := strings.Repeat("a", nameMax+50)
	p1, err := FifoPath("/run/embedshim/io", longID, StreamStdout)
	if err != nil {
		t.Fatal(err)
	}
	// No component may exceed NAME_MAX.
	for _, comp := range strings.Split(p1, "/") {
		if len(comp) > nameMax {
			t.Fatalf("component %q exceeds NAME_MAX (%d)", comp[:20], len(comp))
		}
	}
	if !strings.Contains(p1, "sha256-") {
		t.Fatalf("long id must be replaced by a hash, got %q", p1)
	}
	// Deterministic: same id maps to the same path.
	p2, _ := FifoPath("/run/embedshim/io", longID, StreamStdout)
	if p1 != p2 {
		t.Fatalf("hashing is not deterministic: %q != %q", p1, p2)
	}
}

func TestFifoPathVeryLongFullPathIsHashed(t *testing.T) {
	// Component within NAME_MAX, but the total path over the soft PATH_MAX.
	dir := "/tmp/" + strings.Repeat("d", 3900)
	id := strings.Repeat("b", 100)
	p, err := FifoPath(dir, id, StreamStdout)
	if err != nil {
		t.Fatal(err)
	}
	if len(p) > pathMaxSoft {
		t.Fatalf("path still too long: %d", len(p))
	}
	if !strings.Contains(p, "sha256-") {
		t.Fatalf("over-long full path must trigger hashing, got %q", p)
	}
}

func TestSanitizeIDStripsSeparators(t *testing.T) {
	if got := sanitizeID("../escape"); !strings.Contains(got, ".._escape") {
		t.Fatalf("separator not stripped: %q", got)
	}
}
