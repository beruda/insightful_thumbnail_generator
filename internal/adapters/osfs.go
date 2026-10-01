// Package adapters holds the production implementations of the domain interfaces: the local file
// system, ffmpeg, scratch space, blob store, SQLite repository and checkpoint file.
package adapters

import (
	"context"
	"os"
	"path/filepath"

	"github.com/vukkremic/thumbnailer/internal/domain"
)

// OSFileSystem implements domain.FileSystem over a local POSIX file system.
type OSFileSystem struct{}

func (OSFileSystem) ListFiles(_ context.Context, dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	paths := make([]string, len(entries))
	for i, e := range entries {
		paths[i] = filepath.Join(dir, e.Name())
	}
	return paths, nil
}

func (OSFileSystem) OpenFile(_ context.Context, path string) (domain.FileInfo, error) {
	link, err := os.Lstat(path)
	if err != nil {
		return domain.FileInfo{}, err
	}
	isSymlink := link.Mode()&os.ModeSymlink != 0
	target := link
	if isSymlink {
		if target, err = os.Stat(path); err != nil { // a dangling link returns ErrNotExist, so it's skipped
			return domain.FileInfo{}, err
		}
	}
	canonical := path
	// Directories are always canonicalized, so loop detection works even if the root path itself
	// contains a symlink (e.g. /tmp → /private/tmp on macOS).
	if isSymlink || target.IsDir() {
		if canonical, err = filepath.EvalSymlinks(path); err != nil {
			return domain.FileInfo{}, err
		}
	}
	return domain.FileInfo{
		Path:          path,
		Name:          filepath.Base(path),
		Permissions:   uint32(target.Mode().Perm()),
		Size:          target.Size(),
		IsDir:         target.IsDir(),
		IsSymlink:     isSymlink,
		CanonicalPath: canonical,
		ModTime:       target.ModTime(),
	}, nil
}
