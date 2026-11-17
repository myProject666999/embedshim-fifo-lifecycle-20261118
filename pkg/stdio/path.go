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
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
)

// nameMax bounds a single path component. 255 is the portable NAME_MAX value
// supported by every relevant Linux filesystem.
const nameMax = 255

// pathMaxSoft is a conservative bound on the full path. PATH_MAX is 4096 on
// Linux; staying well below it also leaves room for Mkfifo's internal
// "path/." lookups and for a directory prefix that may itself be long.
const pathMaxSoft = 4000

// ErrSymlinkNotAllowed is returned when a fifo path component resolves to a
// symlink. The fifo directory is shim-controlled state and symlinks there are
// treated as tampering rather than followed.
var ErrSymlinkNotAllowed = errors.New("stdio: symlink is not allowed at fifo path")

// ErrUnsupported is returned by the fifo adapter on platforms without named
// pipe support.
var ErrUnsupported = errors.New("stdio: fifo is not supported on this platform")

// FifoPath returns the path of the fifo for one logical stream of a task.
//
// The layout is <dir>/<id>/<stream>. When the id component would exceed
// NAME_MAX, or the resulting path would exceed the safe PATH_MAX budget, the
// id is replaced by its deterministic SHA-256 digest ("sha256-<hex>"). The
// same id always maps to the same path and the hashed component is always
// length-bounded, so long container identifiers can never hit path limits.
func FifoPath(dir, id, stream string) (string, error) {
	if dir == "" || id == "" || stream == "" {
		return "", fmt.Errorf("stdio: dir, id and stream must all be non-empty")
	}
	id = sanitizeID(id)
	base := filepath.Join(dir, id)
	full := filepath.Join(base, stream)
	if len(full) <= pathMaxSoft {
		return full, nil
	}

	// Full path too long: hash the id, which is the only task-controlled,
	// potentially unbounded component. dir and stream are shim constants.
	sum := sha256.Sum256([]byte(id))
	hashed := "sha256-" + hex.EncodeToString(sum[:])
	full = filepath.Join(dir, hashed, stream)
	if len(full) > pathMaxSoft {
		return "", fmt.Errorf("stdio: fifo path still too long after hashing id: %d bytes", len(full))
	}
	return full, nil
}

// sanitizeID bounds a single path component and strips separators so an id
// can never escape the fifo directory.
func sanitizeID(id string) string {
	id = strings.ReplaceAll(id, string(filepath.Separator), "_")
	if len(id) <= nameMax {
		return id
	}
	sum := sha256.Sum256([]byte(id))
	return "sha256-" + hex.EncodeToString(sum[:])
}
