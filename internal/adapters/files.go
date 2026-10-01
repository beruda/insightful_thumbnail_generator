package adapters

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/vukkremic/thumbnailer/internal/scanner"
)

// ScratchDir is domain.ScratchSpace on a local directory (ffmpeg needs a real path to write to).
type ScratchDir struct{ Dir string }

func (s ScratchDir) Allocate(extension string) (string, error) {
	if err := os.MkdirAll(s.Dir, 0o755); err != nil {
		return "", err
	}
	return filepath.Join(s.Dir, randomName()+extension), nil
}

func (s ScratchDir) Read(path string) ([]byte, error) { return os.ReadFile(path) }

func (s ScratchDir) Remove(path string) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return nil
}

// LocalBlobStore stands in for an object store: a directory using the same key layout. Writes go
// to a temp file and are then renamed, so readers never see a half-written thumbnail (object
// stores give that guarantee natively).
type LocalBlobStore struct{ Root string }

func (s LocalBlobStore) Put(_ context.Context, key string, data []byte, _ string) error {
	dest, err := s.PathFor(key)
	if err != nil {
		return err
	}
	return writeAtomic(dest, data)
}

func (s LocalBlobStore) PathFor(key string) (string, error) {
	if !filepath.IsLocal(key) || strings.Contains(key, "\\") {
		return "", fmt.Errorf("invalid blob key %q", key)
	}
	return filepath.Join(s.Root, key), nil
}

// FileCheckpoints is a scanner.CheckpointStore backed by one JSON file, written atomically.
type FileCheckpoints struct{ Path string }

func (c FileCheckpoints) Load(context.Context) (*scanner.Checkpoint, error) {
	data, err := os.ReadFile(c.Path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var cp scanner.Checkpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		return nil, fmt.Errorf("corrupt checkpoint %s: %w", c.Path, err)
	}
	return &cp, nil
}

func (c FileCheckpoints) Save(_ context.Context, cp scanner.Checkpoint) error {
	data, err := json.Marshal(cp)
	if err != nil {
		return err
	}
	return writeAtomic(c.Path, data)
}

func (c FileCheckpoints) Clear(context.Context) error {
	if err := os.Remove(c.Path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return nil
}

func writeAtomic(dest string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		return err
	}
	tmp := dest + "." + randomName() + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, dest); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

func randomName() string {
	b := make([]byte, 12)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
