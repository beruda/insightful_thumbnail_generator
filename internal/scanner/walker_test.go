package scanner_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"pgregory.net/rapid"

	"github.com/vukkremic/thumbnailer/internal/metrics"
	"github.com/vukkremic/thumbnailer/internal/scanner"
	tk "github.com/vukkremic/thumbnailer/internal/testkit"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

func opts(m *metrics.Counters) scanner.WalkerOptions {
	if m == nil {
		m = metrics.New()
	}
	return scanner.WalkerOptions{Concurrency: 4, Logger: quiet, Metrics: m}
}

func collect(w *scanner.Walker) []string {
	var out []string
	for f := range w.Files(context.Background()) {
		out = append(out, f.Path)
	}
	slices.Sort(out)
	return out
}

func TestWalkerTraversal(t *testing.T) {
	t.Run("empty root yields nothing", func(t *testing.T) {
		tk.Equal(t, collect(scanner.NewWalker(tk.NewFS().Dir("/root"), "/root", opts(nil))), nil, "files")
	})

	t.Run("yields every file in a nested tree, and no directories", func(t *testing.T) {
		fs := tk.NewFS().File("/root/a.mp4").File("/root/sub/b.mov").File("/root/sub/deeper/c.txt").Dir("/root/emptyDir")
		tk.Equal(t, collect(scanner.NewWalker(fs, "/root", opts(nil))),
			[]string{"/root/a.mp4", "/root/sub/b.mov", "/root/sub/deeper/c.txt"}, "files")
	})

	t.Run("very deep nesting is fine (iterative, no recursion)", func(t *testing.T) {
		deep := "/root/" + strings.Repeat("d/", 3000) + "bottom.mp4"
		tk.Equal(t, collect(scanner.NewWalker(tk.NewFS().File(deep), "/root", opts(nil))), []string{deep}, "files")
	})

	t.Run("very wide directories", func(t *testing.T) {
		fs := tk.NewFS()
		for i := range 20_000 {
			fs.File(fmt.Sprintf("/root/v%d.mp4", i))
		}
		got := collect(scanner.NewWalker(fs, "/root", opts(nil)))
		tk.Equal(t, len(got), 20_000, "count")
		tk.Equal(t, len(slices.Compact(got)), 20_000, "unique count")
	})

	t.Run("stops with the context error when cancelled", func(t *testing.T) {
		fs := tk.NewFS().File("/root/a.mp4").File("/root/sub/b.mp4")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		w := scanner.NewWalker(fs, "/root", opts(nil))
		for range w.Files(ctx) {
			cancel()
		}
		tk.True(t, errors.Is(w.Err(), context.Canceled), "Err() = %v, want context.Canceled", w.Err())
	})
}

func TestWalkerIsLazy(t *testing.T) {
	t.Run("does no I/O beyond what the consumer pulls", func(t *testing.T) {
		fs := tk.NewFS()
		for i := range 1000 {
			fs.File(fmt.Sprintf("/root/v%04d.mp4", i))
		}
		o := opts(nil)
		o.Concurrency = 8
		n := 0
		for range scanner.NewWalker(fs, "/root", o).Files(context.Background()) {
			if n++; n == 3 {
				break
			}
		}
		lists, opens := fs.Calls()
		tk.Equal(t, lists, 1, "list calls")
		tk.Equal(t, opens, 8, "open calls (one chunk; the other 992 files were never touched)")
	})

	t.Run("never exceeds the OpenFile concurrency limit", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			fs := tk.NewFS().WithLatency(5 * time.Millisecond)
			for i := range 100 {
				fs.File(fmt.Sprintf("/root/v%d.mp4", i))
			}
			o := opts(nil)
			o.Concurrency = 3
			tk.Equal(t, len(collect(scanner.NewWalker(fs, "/root", o))), 100, "files")
			tk.Equal(t, fs.PeakInFlight(), 3, "peak concurrent calls")
		})
	})
}

func TestWalkerErrorIsolation(t *testing.T) {
	t.Run("an unlistable directory is skipped, the rest continues", func(t *testing.T) {
		fs := tk.NewFS().File("/root/locked/secret.mp4").File("/root/open/ok.mp4").FailList("/root/locked", nil)
		m := metrics.New()
		tk.Equal(t, collect(scanner.NewWalker(fs, "/root", opts(m))), []string{"/root/open/ok.mp4"}, "files")
		tk.Equal(t, m.Get("scanner.list_errors"), 1, "list errors")
	})

	t.Run("a file that vanished between list and open is skipped quietly", func(t *testing.T) {
		fs := tk.NewFS().File("/root/gone.mp4").File("/root/here.mp4").VanishOnOpen("/root/gone.mp4")
		m := metrics.New()
		tk.Equal(t, collect(scanner.NewWalker(fs, "/root", opts(m))), []string{"/root/here.mp4"}, "files")
		tk.Equal(t, m.Get("scanner.vanished"), 1, "vanished")
		tk.Equal(t, m.Get("scanner.open_errors"), 0, "open errors")
	})

	t.Run("a file that can't be opened is skipped and counted", func(t *testing.T) {
		fs := tk.NewFS().File("/root/bad.mp4").File("/root/ok.mp4").FailOpen("/root/bad.mp4", nil)
		m := metrics.New()
		tk.Equal(t, collect(scanner.NewWalker(fs, "/root", opts(m))), []string{"/root/ok.mp4"}, "files")
		tk.Equal(t, m.Get("scanner.open_errors"), 1, "open errors")
	})

	t.Run("a missing root yields nothing rather than failing", func(t *testing.T) {
		tk.Equal(t, collect(scanner.NewWalker(tk.NewFS(), "/nope", opts(nil))), nil, "files")
	})
}

func TestWalkerSymlinks(t *testing.T) {
	loopTree := func() *tk.FS {
		return tk.NewFS().
			File("/root/a.mp4").
			File("/root/sub/b.mp4").
			Symlink("/root/sub/backToRoot", "/root").
			Symlink("/root/sub/linkedFile", "/elsewhere/c.mp4").
			File("/elsewhere/c.mp4")
	}

	t.Run("ignored by default", func(t *testing.T) {
		tk.Equal(t, collect(scanner.NewWalker(loopTree(), "/root", opts(nil))),
			[]string{"/root/a.mp4", "/root/sub/b.mp4"}, "files")
	})

	t.Run("loops terminate when following symlinks; each directory visited once", func(t *testing.T) {
		m := metrics.New()
		o := opts(m)
		o.FollowSymlinks = true
		tk.Equal(t, collect(scanner.NewWalker(loopTree(), "/root", o)),
			[]string{"/root/a.mp4", "/root/sub/b.mp4", "/root/sub/linkedFile"}, "files")
		tk.Equal(t, m.Get("scanner.symlink_loops"), 1, "loops detected")
	})

	t.Run("dangling symlinks are skipped", func(t *testing.T) {
		fs := tk.NewFS().Symlink("/root/dangling", "/missing.mp4").File("/root/ok.mp4")
		o := opts(nil)
		o.FollowSymlinks = true
		tk.Equal(t, collect(scanner.NewWalker(fs, "/root", o)), []string{"/root/ok.mp4"}, "files")
	})
}

func TestWalkerResume(t *testing.T) {
	tree := func() *tk.FS {
		return tk.NewFS().File("/root/a.mp4").File("/root/b.mp4").File("/root/x/c.mp4").File("/root/x/d.mp4").
			File("/root/x/y/e.mp4").File("/root/z/f.mp4").File("/root/g.mp4")
	}

	for consumed := range 8 {
		t.Run(fmt.Sprintf("after %d files: exactly the remaining files", consumed), func(t *testing.T) {
			fs := tree()
			all := collect(scanner.NewWalker(fs, "/root", opts(nil)))
			seen, pos := walkPartially(fs, consumed)
			rest := collect(scanner.ResumeWalker(fs, pos, opts(nil)))

			tk.Equal(t, sorted(append(seen, rest...)), all, "seen + rest (nothing missing)")
			for _, p := range seen {
				tk.True(t, !slices.Contains(rest, p), "%s was yielded twice", p)
			}
		})
	}

	t.Run("files added to the current directory after the checkpoint are still found", func(t *testing.T) {
		fs := tree()
		seen, pos := walkPartially(fs, 1)
		fs.File("/root/x/new.mp4")
		rest := collect(scanner.ResumeWalker(fs, pos, opts(nil)))
		tk.True(t, slices.Contains(rest, "/root/x/new.mp4"), "new file missing from %v", rest)
		tk.True(t, !slices.Contains(rest, seen[0]), "first file repeated")
	})

	t.Run("property: for any tree and any stopping point, every file is yielded exactly once", func(t *testing.T) {
		rapid.Check(t, func(rt *rapid.T) {
			fs := tk.NewFS().Dir("/root")
			files := genTree(rt, fs, "/root", 0)
			stopAfter := rapid.IntRange(0, 25).Draw(rt, "stopAfter")

			seen, pos := walkPartially(fs, stopAfter)
			rest := collect(scanner.ResumeWalker(fs, pos, opts(nil)))
			got := sorted(append(seen, rest...))
			if !slices.Equal(got, sorted(files)) {
				rt.Fatalf("got %v, want %v", got, sorted(files))
			}
		})
	})
}

// walkPartially consumes n files, then "crashes": it returns what it saw and a JSON-round-tripped
// position, exactly as a checkpoint would be persisted.
func walkPartially(fs *tk.FS, n int) ([]string, scanner.Position) {
	w := scanner.NewWalker(fs, "/root", opts(nil))
	var seen []string
	if n > 0 {
		for f := range w.Files(context.Background()) {
			seen = append(seen, f.Path)
			if len(seen) == n {
				break
			}
		}
	}
	data, _ := json.Marshal(w.Position())
	var pos scanner.Position
	_ = json.Unmarshal(data, &pos)
	return seen, pos
}

func genTree(rt *rapid.T, fs *tk.FS, dir string, depth int) []string {
	var files []string
	names := rapid.SliceOfNDistinct(rapid.StringMatching(`[a-z]{1,3}`), 0, 4, rapid.ID[string]).Draw(rt, "names")
	for _, name := range names {
		p := dir + "/" + name
		if depth < 3 && rapid.Bool().Draw(rt, "isDir") {
			fs.Dir(p)
			files = append(files, genTree(rt, fs, p, depth+1)...)
		} else {
			fs.File(p)
			files = append(files, p)
		}
	}
	return files
}

func sorted(s []string) []string {
	out := slices.Clone(s)
	slices.Sort(out)
	return out
}
