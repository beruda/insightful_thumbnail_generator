package app_test

import (
	"context"
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	"github.com/vukkremic/thumbnailer/internal/app"
	"github.com/vukkremic/thumbnailer/internal/domain"
	tk "github.com/vukkremic/thumbnailer/internal/testkit"
)

func withVideos(h *tk.Harness, n int) *tk.Harness {
	for i := range n {
		h.FS.File(fmt.Sprintf("/videos/v%03d.mp4", i))
	}
	return h
}

func run(t *testing.T, h *tk.Harness, ctx context.Context) {
	t.Helper()
	tk.Equal(t, h.Service().Run(ctx), nil, "Run error")
}

func readyCount(h *tk.Harness) int {
	n := 0
	for _, r := range h.Repo.All() {
		if r.Status == domain.StatusReady {
			n++
		}
	}
	return n
}

// The whole service (role=all), wired exactly as main does it, with fakes.
func TestServiceOneShot(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := withVideos(tk.NewHarness(), 25)
		h.FS.File("/videos/readme.txt").File("/videos/nested/x.mov")

		run(t, h, context.Background())

		tk.Equal(t, readyCount(h), 26, "ready")
		tk.Equal(t, len(h.Events.Published()), 26, "events")
		tk.Equal(t, h.Queue.Stats(), (queueStats{}), "queue drained")
		tk.Equal(t, h.Metrics.Get("worker.generated"), 26, "generated")
		tk.Equal(t, h.Metrics.Get("scanner.skipped.not_video"), 1, "skipped")
	})
}

func TestServiceConcurrencyCap(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := withVideos(tk.NewHarness(tk.Configure(func(c *app.Config) { c.Worker.Concurrency = 4 })), 40)
		h.Launcher.SetDefault(tk.SucceedAfter(time.Second))
		start := time.Now()

		run(t, h, context.Background())

		tk.Equal(t, h.Launcher.PeakRunning(), 4, "peak concurrent ffmpeg")
		tk.Equal(t, time.Since(start), 10*time.Second, "elapsed: 40 jobs / 4 slots × 1s, so the pool stayed full")
	})
}

func TestServiceRetries(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := withVideos(tk.NewHarness(), 1)
		path := "/videos/v000.mp4"
		h.Launcher.Script(path, tk.ExitWith(1, tk.TransientStderr), tk.ExitWith(1, tk.TransientStderr), tk.Succeed())
		start := time.Now()

		run(t, h, context.Background())

		tk.Equal(t, h.Launcher.SpawnsFor(path), 3, "attempts")
		rec, _, _ := h.Repo.MemoryRepository.Get(context.Background(), path)
		tk.Equal(t, [2]any{rec.Status, rec.Attempts}, [2]any{domain.StatusReady, 3}, "record")
		// Two backoff waits ([0.5,1]s + [1,2]s) plus 3 × 10ms of process time.
		elapsed := time.Since(start)
		tk.True(t, elapsed >= 1530*time.Millisecond && elapsed <= 3030*time.Millisecond, "elapsed %s", elapsed)
	})
}

func TestServiceHungJobDoesNotBlockOthers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := withVideos(tk.NewHarness(tk.Configure(func(c *app.Config) { c.Worker.Concurrency = 2 })), 10)
		h.Launcher.Script("/videos/v000.mp4", tk.Hang(0), tk.Succeed())

		run(t, h, context.Background())

		tk.Equal(t, readyCount(h), 10, "ready")
		var order []string
		for _, e := range h.Logs.Entries() {
			if e.Message == "thumbnail generated" {
				order = append(order, e.Attrs["path"].(string))
			}
		}
		tk.Equal(t, order[len(order)-1], "/videos/v000.mp4", "the hung job finished last")
		tk.Equal(t, len(h.Launcher.Leaked()), 0, "leaked")
	})
}

func TestServiceSecondRunSkipsEverything(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := withVideos(tk.NewHarness(), 10)
		run(t, h, context.Background())
		spawned := len(h.Launcher.Processes())

		tk.Equal(t, h.Restart().Service.Run(context.Background()), nil, "second run")

		tk.Equal(t, len(h.Launcher.Processes()), spawned, "ffmpeg runs after the second pass")
		tk.Equal(t, h.Metrics.Get("scanner.skipped.up_to_date"), 10, "skipped")
	})
}

func TestServicePermanentFailures(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := withVideos(tk.NewHarness(), 5)
		h.Launcher.Script("/videos/v002.mp4", tk.ExitWith(1, "moov atom not found"))

		run(t, h, context.Background())

		dead := h.Queue.DeadLetters()
		tk.Equal(t, len(dead), 1, "dead letters")
		tk.Equal(t, dead[0].Message.Path, "/videos/v002.mp4", "dead-lettered path")
		tk.Equal(t, readyCount(h), 4, "ready")
	})
}

func TestServiceGracefulShutdown(t *testing.T) {
	shutdownAfter := func(d time.Duration) context.Context {
		ctx, cancel := context.WithCancel(context.Background())
		time.AfterFunc(d, cancel)
		return ctx
	}

	t.Run("no grace period: in-flight ffmpeg killed, jobs requeued, no temp files", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			h := withVideos(tk.NewHarness(tk.Configure(func(c *app.Config) {
				c.Worker.Concurrency, c.Worker.ShutdownGrace = 3, 0
			})), 10)
			h.Launcher.SetDefault(tk.SucceedAfter(5 * time.Second))

			run(t, h, shutdownAfter(time.Second))

			procs := h.Launcher.Processes()
			tk.Equal(t, len(procs), 3, "processes started")
			for _, p := range procs {
				tk.Equal(t, p.State, tk.Killed, "state of "+p.VideoPath)
			}
			tk.Equal(t, len(h.Scratch.Files()), 0, "scratch files")
			tk.Equal(t, len(h.Repo.All()), 0, "records")
			tk.Equal(t, len(h.Queue.Pending()), 10, "nothing lost: 3 released + 7 never started")
			tk.Equal(t, h.Metrics.Get("worker.released"), 3, "released")
		})
	})

	t.Run("with a grace period: in-flight jobs finish, no new ones start", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			h := withVideos(tk.NewHarness(tk.Configure(func(c *app.Config) {
				c.Worker.Concurrency, c.Worker.ShutdownGrace = 3, 10*time.Second
			})), 10)
			h.Launcher.SetDefault(tk.SucceedAfter(5 * time.Second))
			start := time.Now()

			run(t, h, shutdownAfter(time.Second))

			tk.Equal(t, readyCount(h), 3, "ready")
			tk.Equal(t, len(h.Queue.Pending()), 7, "still queued")
			tk.Equal(t, time.Since(start), 5*time.Second, "stopped when in-flight work finished, not at the deadline")
		})
	})

	t.Run("jobs that outlive the grace period are killed", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			h := withVideos(tk.NewHarness(tk.Configure(func(c *app.Config) { c.Worker.ShutdownGrace = 2 * time.Second })), 2)
			h.Launcher.SetDefault(tk.SucceedAfter(30 * time.Second))
			start := time.Now()

			run(t, h, shutdownAfter(time.Second))

			tk.Equal(t, time.Since(start), 3*time.Second, "elapsed")
			for _, p := range h.Launcher.Processes() {
				tk.Equal(t, p.State, tk.Killed, "state")
			}
			tk.Equal(t, len(h.Queue.Pending()), 2, "requeued")
		})
	})
}

func TestServiceLongRunning(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := withVideos(tk.NewHarness(tk.Configure(func(c *app.Config) { c.Once, c.ScanInterval = false, time.Minute })), 2)
		time.AfterFunc(30*time.Second, func() { h.FS.File("/videos/new.mp4") })
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()

		run(t, h, ctx)

		tk.Equal(t, readyCount(h), 3, "ready")
		tk.Equal(t, h.Metrics.Get("scanner.passes_completed"), 2, "passes")
		tk.Equal(t, len(h.Launcher.Processes()), 3, "the first two weren't regenerated on the second pass")
	})
}

func TestServiceRoles(t *testing.T) {
	t.Run("role=scanner only publishes", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			h := withVideos(tk.NewHarness(tk.Configure(func(c *app.Config) { c.Role = app.RoleScanner })), 3)
			run(t, h, context.Background())
			tk.Equal(t, len(h.Queue.Pending()), 3, "published")
			tk.Equal(t, len(h.Launcher.Processes()), 0, "ffmpeg runs")
		})
	})

	t.Run("role=worker only consumes", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			h := withVideos(tk.NewHarness(tk.Configure(func(c *app.Config) { c.Role = app.RoleWorker })), 3)
			for _, p := range []string{"/videos/v000.mp4", "/videos/v001.mp4"} {
				_ = h.Queue.Publish(context.Background(), domain.VideoJob{Path: p, Size: 1000, SourceVersion: "1000-1000"})
			}
			h.Queue.Close()
			run(t, h, context.Background())
			tk.Equal(t, readyCount(h), 2, "ready")
			tk.Equal(t, h.Metrics.Get("scanner.files_seen"), 0, "scanned")
		})
	})
}

func TestServiceFatalScannerErrorStopsWorkers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := withVideos(tk.NewHarness(tk.Configure(func(c *app.Config) { c.Once = false })), 3)
		h.Config.Scanner.Concurrency = 0 // invalid: the scanner fails at startup
		err := h.Service().Run(context.Background())
		// Without the shared cancel, the long-running worker would wait forever and synctest would
		// report a deadlock instead of returning.
		tk.True(t, err != nil, "want the scanner error, got nil")
	})
}
