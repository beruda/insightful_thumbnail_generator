package adapters_test

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/vukkremic/thumbnailer/internal/adapters"
	"github.com/vukkremic/thumbnailer/internal/domain"
	"github.com/vukkremic/thumbnailer/internal/metrics"
	"github.com/vukkremic/thumbnailer/internal/queue"
	"github.com/vukkremic/thumbnailer/internal/scanner"
	"github.com/vukkremic/thumbnailer/internal/storage"
	tk "github.com/vukkremic/thumbnailer/internal/testkit"
)

func write(t *testing.T, path string, data string) {
	t.Helper()
	tk.Equal(t, os.MkdirAll(filepath.Dir(path), 0o755), nil, "mkdir")
	tk.Equal(t, os.WriteFile(path, []byte(data), 0o644), nil, "write")
}

func TestOSFileSystem(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	write(t, filepath.Join(root, "a.mp4"), "video")
	write(t, filepath.Join(root, "b.mp4"), "video")
	write(t, filepath.Join(root, "sub", "c.mov"), "video")
	tk.Equal(t, os.Symlink(root, filepath.Join(root, "sub", "loop")), nil, "symlink")
	tk.Equal(t, os.Symlink(filepath.Join(root, "missing"), filepath.Join(root, "dangling")), nil, "symlink")

	listAll := func(fsys adapters.OSFileSystem, dir string) (names []string, pages int) {
		token := ""
		for {
			page, err := fsys.ListPage(ctx, dir, token)
			tk.Equal(t, err, nil, "ListPage")
			pages++
			for _, e := range page.Entries {
				names = append(names, e.Name)
			}
			if token = page.NextToken; token == "" {
				return names, pages
			}
		}
	}

	t.Run("lists children with metadata, symlinks flagged and not followed", func(t *testing.T) {
		page, err := adapters.OSFileSystem{}.ListPage(ctx, root, "")
		tk.Equal(t, err, nil, "error")
		byName := map[string]domain.FileInfo{}
		for _, e := range page.Entries {
			byName[e.Name] = e
		}
		tk.Equal(t, len(byName), 4, "entries")
		a := byName["a.mp4"]
		tk.Equal(t, [5]any{a.Path, a.Size, a.IsDir, a.IsSymlink, a.Permissions},
			[5]any{filepath.Join(root, "a.mp4"), int64(5), false, false, uint32(0o644)}, "a.mp4")
		tk.True(t, time.Since(a.ModTime) < time.Minute, "mtime %v", a.ModTime)
		tk.True(t, byName["sub"].IsDir, "sub is a directory")
		tk.True(t, byName["dangling"].IsSymlink, "dangling is flagged as a symlink")
		tk.Equal(t, page.NextToken, "", "single page")
	})

	t.Run("paginates in name order", func(t *testing.T) {
		names, pages := listAll(adapters.OSFileSystem{PageSize: 2}, root)
		tk.Equal(t, names, []string{"a.mp4", "b.mp4", "dangling", "sub"}, "names")
		tk.Equal(t, pages, 2, "pages")
	})

	t.Run("a missing directory is reported as not found", func(t *testing.T) {
		_, err := adapters.OSFileSystem{}.ListPage(ctx, filepath.Join(root, "nope"), "")
		tk.True(t, errors.Is(err, fs.ErrNotExist), "err = %v", err)
	})

	t.Run("the scanner skips a real symlink loop", func(t *testing.T) {
		videos := queue.NewMemory[domain.VideoJob](100)
		s := scanner.New(scanner.Deps{
			FS: adapters.OSFileSystem{}, Dirs: queue.NewUnbounded[scanner.DirJob](), Videos: videos,
			Repository: storage.NewMemoryRepository(), Logger: quiet(), Metrics: metrics.New(),
		}, scanner.Config{Root: root, Extensions: scanner.DefaultVideoExtensions, Concurrency: 4})
		tk.Equal(t, s.ScanOnce(ctx), nil, "ScanOnce")
		var names []string
		for _, j := range videos.Pending() {
			names = append(names, filepath.Base(j.Path))
		}
		slices.Sort(names)
		tk.Equal(t, names, []string{"a.mp4", "b.mp4", "c.mov"}, "published")
	})
}

func TestLocalBlobStore(t *testing.T) {
	store := adapters.LocalBlobStore{Root: t.TempDir()}
	ctx := context.Background()

	tk.Equal(t, store.Put(ctx, "thumbnails/ab/abc/1.jpg", []byte("one"), "image/jpeg"), nil, "put")
	tk.Equal(t, store.Put(ctx, "thumbnails/ab/abc/1.jpg", []byte("two"), "image/jpeg"), nil, "overwrite")
	p, _ := store.PathFor("thumbnails/ab/abc/1.jpg")
	data, _ := os.ReadFile(p)
	tk.Equal(t, string(data), "two", "contents")

	entries, _ := os.ReadDir(filepath.Dir(p))
	tk.Equal(t, len(entries), 1, "no temp files left behind")

	for _, bad := range []string{"../escape.jpg", "/abs.jpg", "a/../../b.jpg"} {
		tk.True(t, store.Put(ctx, bad, nil, "") != nil, "key %q should be rejected", bad)
	}
}

func TestSQLiteRepository(t *testing.T) {
	ctx := context.Background()
	repo, err := adapters.OpenSQLite(filepath.Join(t.TempDir(), "db.sqlite"))
	tk.Equal(t, err, nil, "open")
	defer repo.Close()

	_, found, err := repo.Get(ctx, "/missing.mp4")
	tk.Equal(t, [2]any{found, err}, [2]any{false, error(nil)}, "missing record")

	rec := domain.ThumbnailRecord{
		VideoPath: "/a.mp4", SourceVersion: "10-1", Status: domain.StatusReady, BlobKey: "k", ByteLength: 3,
		ContentType: "image/jpeg", Attempts: 2, Notified: true, UpdatedAt: time.UnixMilli(1_700_000_000_000),
	}
	tk.Equal(t, repo.Upsert(ctx, rec), nil, "insert")
	got, _, _ := repo.Get(ctx, "/a.mp4")
	tk.Equal(t, got, rec, "round trip")

	other := rec
	other.VideoPath = "/b.mp4"
	tk.Equal(t, repo.Upsert(ctx, other), nil, "insert second")
	paths := []string{"/a.mp4", "/b.mp4", "/missing.mp4"}
	for i := range 1200 {
		paths = append(paths, fmt.Sprintf("/x%d.mp4", i)) // spans several IN (...) chunks
	}
	many, err := repo.GetMany(ctx, paths)
	tk.Equal(t, err, nil, "GetMany")
	tk.Equal(t, many, map[string]domain.ThumbnailRecord{"/a.mp4": rec, "/b.mp4": other}, "GetMany results")

	rec.Status, rec.FailureKind, rec.FailureReason, rec.Notified = domain.StatusFailed, "corrupt_input", "moov atom not found", false
	tk.Equal(t, repo.Upsert(ctx, rec), nil, "update")
	got, _, _ = repo.Get(ctx, "/a.mp4")
	tk.Equal(t, got, rec, "updated round trip")
}

func TestScratchDir(t *testing.T) {
	s := adapters.ScratchDir{Dir: filepath.Join(t.TempDir(), "scratch")}
	p, err := s.Allocate(".jpg")
	tk.Equal(t, err, nil, "allocate")
	_, err = s.Read(p)
	tk.True(t, errors.Is(err, fs.ErrNotExist), "unwritten file reads as not found")
	tk.Equal(t, s.Remove(p), nil, "removing a missing file is a no-op")
}
