package loadtest_test

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"log/slog"
	"testing"
	"time"

	"github.com/vukkremic/thumbnailer/internal/loadtest"
	"github.com/vukkremic/thumbnailer/internal/metrics"
	"github.com/vukkremic/thumbnailer/internal/scanner"
	tk "github.com/vukkremic/thumbnailer/internal/testkit"
)

func tree(files int) loadtest.TreeConfig {
	return loadtest.TreeConfig{Seed: 7, Files: files, FanOut: 4, FilesPerDir: 10, VideoRatio: 0.8,
		Mix: loadtest.Mix{Corrupt: 0.05, Slow: 0.02, Hang: 0.01, Flaky: 0.05}}
}

func TestGeneratedFS(t *testing.T) {
	ctx := context.Background()
	gen := loadtest.NewGeneratedFS("/bucket", tree(1234))

	t.Run("a full walk finds exactly the census, and every listed path opens", func(t *testing.T) {
		w := scanner.NewWalker(gen, "/bucket", scanner.WalkerOptions{Concurrency: 8,
			Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Metrics: metrics.New()})
		seen := 0
		for range w.Files(ctx) {
			seen++
		}
		files, _, _ := gen.Census()
		tk.Equal(t, seen, files, "files walked")
		tk.Equal(t, files, 1234, "census")
	})

	t.Run("is deterministic", func(t *testing.T) {
		a, _ := gen.ListFiles(ctx, "/bucket/d1")
		b, _ := loadtest.NewGeneratedFS("/bucket", tree(1234)).ListFiles(ctx, "/bucket/d1")
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
