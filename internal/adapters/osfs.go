// Package adapters holds the production implementations of the domain interfaces: the local file
// system, ffmpeg, scratch space, blob store and SQLite repository.
package adapters

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"sort"

	"github.com/vukkremic/thumbnailer/internal/domain"
)

const defaultPageSize = 1000

// OSFileSystem implements domain.FileSystem over a local POSIX file system.
//
// POSIX has no native listing cursor that survives across calls, so each page re-reads the
// directory (sorted by name) and resumes after the token, which is the last name of the previous
// page. That's O(n) per page, fine for local directories; object stores page natively.
type OSFileSystem struct {
	PageSize int // entries per page; 0 means 1000
}

func (o OSFileSystem) ListPage(_ context.Context, dir, token string) (domain.Page, error) {
	entries, err := os.ReadDir(dir) // sorted by name
	if err != nil {
		return domain.Page{}, err
	}
	pageSize := o.PageSize
	if pageSize <= 0 {
		pageSize = defaultPageSize
	}
	start := 0
	if token != "" {
		start = sort.Search(len(entries), func(i int) bool { return entries[i].Name() > token })
	}
	end := min(start+pageSize, len(entries))

	page := domain.Page{Entries: make([]domain.FileInfo, 0, end-start)}
	for _, e := range entries[start:end] {
		info, err := e.Info() // lstat: a symlink describes itself, not its target
		if errors.Is(err, fs.ErrNotExist) {
			continue // deleted since ReadDir
		}
		if err != nil {
			return domain.Page{}, err
		}
		page.Entries = append(page.Entries, fileInfo(filepath.Join(dir, e.Name()), info))
	}
	if end < len(entries) {
		page.NextToken = entries[end-1].Name()
	}
	return page, nil
}

func (OSFileSystem) OpenFile(_ context.Context, path string) (domain.FileInfo, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return domain.FileInfo{}, err
	}
	return fileInfo(path, info), nil
}

func fileInfo(path string, info fs.FileInfo) domain.FileInfo {
	return domain.FileInfo{
		Path:        path,
		Name:        info.Name(),
		Permissions: uint32(info.Mode().Perm()),
		Size:        info.Size(),
		IsDir:       info.IsDir(),
		IsSymlink:   info.Mode()&fs.ModeSymlink != 0,
		ModTime:     info.ModTime(),
	}
}
