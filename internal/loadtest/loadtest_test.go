package loadtest_test

import (
	"context"
	"errors"
	"io/fs"
	"testing"
	"time"

	"github.com/vukkremic/thumbnailer/internal/loadtest"
	tk "github.com/vukkremic/thumbnailer/internal/testkit"
)

func tree(files int) loadtest.TreeConfig {
	return loadtest.TreeConfig{Seed: 7, Files: files, FanOut: 4, FilesPerDir: 10, PageSize: 3, VideoRatio: 0.8,
		Mix: loadtest.Mix{Corrupt: 0.05, Slow: 0.02, Hang: 0.01, Flaky: 0.05}}
}

func TestGeneratedFS(t *testing.T) {
	ctx := context.Background()
	gen := loadtest.NewGeneratedFS("/bucket", tree(1234))

	t.Run("listing every page finds exactly the census, and every entry opens with the same metadata", func(t *testing.T) {
		files, dirs := 0, 0
		stack := []string{"/bucket"}
		for len(stack) > 0 {
			dir := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			dirs++
			for token := ""; ; {
				page, err := gen.ListPage(ctx, dir, token)
				tk.Equal(t, err, nil, "ListPage "+dir)
				for _, e := range page.Entries {
					opened, err := gen.OpenFile(ctx, e.Path)
					tk.Equal(t, err, nil, "OpenFile "+e.Path)
					tk.Equal(t, opened, e, "metadata of "+e.Path)
					if e.IsDir {
						stack = append(stack, e.Path)
					} else {
						files++
					}
				}
				if token = page.NextToken; token == "" {
					break
				}
			}
		}
		census, _, _ := gen.Census()
		tk.Equal(t, files, census, "files listed")
		tk.Equal(t, census, 1234, "census")
		tk.Equal(t, dirs, gen.Dirs(), "directories listed")
	})

	t.Run("is deterministic", func(t *testing.T) {
		a, _ := gen.ListPage(ctx, "/bucket/d1", "")
		b, _ := loadtest.NewGeneratedFS("/bucket", tree(1234)).ListPage(ctx, "/bucket/d1", "")
		tk.Equal(t, a, b, "listing")
	})

	t.Run("paths outside the tree don't exist", func(t *testing.T) {
		for _, p := range []string{"/bucket/d999999", "/bucket/d2/d1", "/bucket/f0_999.mp4", "/bucket/nope.mp4", "/other"} {
			_, err := gen.OpenFile(ctx, p)
			tk.True(t, errors.Is(err, fs.ErrNotExist), "%s: err = %v", p, err)
		}
	})
}

func TestRunValidatesEndState(t *testing.T) {
	cfg := loadtest.DefaultConfig()
	cfg.Tree = tree(3000)
	cfg.Work = 0
	cfg.ProcessTimeout = 50 * time.Millisecond
	cfg.Concurrency = 16

	rep, err := loadtest.Run(context.Background(), cfg)
	tk.Equal(t, err, nil, "error")
	tk.True(t, rep.OK, "problems: %v", rep.Problems)
	tk.Equal(t, len(rep.Passes), 2, "passes")
	tk.Equal(t, rep.Passes[1].Published, 0, "second pass publishes nothing")
	tk.Equal(t, rep.Passes[1].SkippedUpToDate, int64(rep.Tree.Videos), "second pass skips every video")
}
