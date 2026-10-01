package adapters_test

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/vukkremic/thumbnailer/internal/adapters"
	"github.com/vukkremic/thumbnailer/internal/domain"
	"github.com/vukkremic/thumbnailer/internal/metrics"
	"github.com/vukkremic/thumbnailer/internal/scanner"
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
	write(t, filepath.Join(root, "sub", "b.mov"), "video")
	tk.Equal(t, os.Symlink(root, filepath.Join(root, "sub", "loop")), nil, "symlink")
	tk.Equal(t, os.Symlink(filepath.Join(root, "missing"), filepath.Join(root, "dangling")), nil, "symlink")
	fsys := adapters.OSFileSystem{}

	t.Run("lists direct children as full paths", func(t *testing.T) {
		children, err := fsys.ListFiles(ctx, root)
		tk.Equal(t, err, nil, "error")
		slices.Sort(children)
		tk.Equal(t, children, []string{filepath.Join(root, "a.mp4"), filepath.Join(root, "dangling"), filepath.Join(root, "sub")}, "children")
	})

	t.Run("reports file metadata", func(t *testing.T) {
		info, err := fsys.OpenFile(ctx, filepath.Join(root, "a.mp4"))
		tk.Equal(t, err, nil, "error")
		tk.Equal(t, [4]any{info.Name, info.Size, info.IsDir, info.Permissions}, [4]any{"a.mp4", int64(5), false, uint32(0o644)}, "info")
		tk.True(t, time.Since(info.ModTime) < time.Minute, "mtime %v", info.ModTime)
	})

	t.Run("a dangling symlink is reported as not found", func(t *testing.T) {
		_, err := fsys.OpenFile(ctx, filepath.Join(root, "dangling"))
		tk.True(t, errors.Is(err, fs.ErrNotExist), "err = %v", err)
	})

	t.Run("the walker terminates on a real symlink loop", func(t *testing.T) {
		w := scanner.NewWalker(fsys, root, scanner.WalkerOptions{Concurrency: 4, FollowSymlinks: true, Logger: quiet(), Metrics: metrics.New()})
		var names []string
		for f := range w.Files(ctx) {
			names = append(names, f.Name)
		}
		slices.Sort(names)
		tk.Equal(t, names, []string{"a.mp4", "b.mov"}, "files")
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

	rec.Status, rec.FailureKind, rec.FailureReason, rec.Notified = domain.StatusFailed, "corrupt_input", "moov atom not found", false
	tk.Equal(t, repo.Upsert(ctx, rec), nil, "update")
	got, _, _ = repo.Get(ctx, "/a.mp4")
	tk.Equal(t, got, rec, "updated round trip")
}

func TestFileCheckpoints(t *testing.T) {
	ctx := context.Background()
	store := adapters.FileCheckpoints{Path: filepath.Join(t.TempDir(), "nested", "cp.json")}

	cp, err := store.Load(ctx)
	tk.True(t, cp == nil && err == nil, "empty store: %v, %v", cp, err)

	want := scanner.Checkpoint{Root: "/r", Position: scanner.Position{Pending: []string{"/r/a"}, Current: &scanner.Cursor{Dir: "/r", After: "/r/x"}}}
	tk.Equal(t, store.Save(ctx, want), nil, "save")
	got, _ := store.Load(ctx)
	tk.Equal(t, *got, want, "round trip")

	tk.Equal(t, store.Clear(ctx), nil, "clear")
	tk.Equal(t, store.Clear(ctx), nil, "clear twice")
	cp, _ = store.Load(ctx)
	tk.True(t, cp == nil, "cleared")

	write(t, store.Path, "{not json")
	_, err = store.Load(ctx)
	tk.True(t, err != nil, "corrupt checkpoint should error")
}

func TestScratchDir(t *testing.T) {
	s := adapters.ScratchDir{Dir: filepath.Join(t.TempDir(), "scratch")}
	p, err := s.Allocate(".jpg")
	tk.Equal(t, err, nil, "allocate")
	_, err = s.Read(p)
	tk.True(t, errors.Is(err, fs.ErrNotExist), "unwritten file reads as not found")
	tk.Equal(t, s.Remove(p), nil, "removing a missing file is a no-op")
}
