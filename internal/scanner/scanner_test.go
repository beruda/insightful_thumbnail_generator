package scanner_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"testing/synctest"

	"github.com/vukkremic/thumbnailer/internal/app"
	"github.com/vukkremic/thumbnailer/internal/domain"
	"github.com/vukkremic/thumbnailer/internal/queue"
	"github.com/vukkremic/thumbnailer/internal/scanner"
	tk "github.com/vukkremic/thumbnailer/internal/testkit"
)

func pending(q *queue.Memory[domain.VideoJob]) []string { return sorted(tk.PendingPaths(q)) }

func TestScannerPublishing(t *testing.T) {
	t.Run("publishes one job per readable, non-empty video and counts the skips", func(t *testing.T) {
		h := tk.NewHarness()
		h.FS.File("/videos/a.mp4", tk.Size(10), tk.ModTimeMs(5)).
			File("/videos/notes.txt").
			File("/videos/empty.mp4", tk.Size(0)).
			File("/videos/locked.mov", tk.Perm(0)).
			File("/videos/sub/B.MKV")

		tk.Equal(t, h.Scanner().ScanOnce(context.Background()), nil, "error")

		tk.Equal(t, pending(h.Queue), []string{"/videos/a.mp4", "/videos/sub/B.MKV"}, "published")
		job := h.Queue.Pending()[slices.IndexFunc(h.Queue.Pending(), func(j domain.VideoJob) bool { return j.Path == "/videos/a.mp4" })]
		tk.Equal(t, job, domain.VideoJob{Path: "/videos/a.mp4", Size: 10, SourceVersion: "10-5"}, "job")
		for name, want := range map[string]int64{
			"scanner.files_seen": 5, "scanner.published": 2, "scanner.skipped.not_video": 1,
			"scanner.skipped.empty": 1, "scanner.skipped.unreadable": 1,
		} {
			tk.Equal(t, h.Metrics.Get(name), want, name)
		}
	})

	t.Run("uses the ETag as the source version when the file system has one", func(t *testing.T) {
		h := tk.NewHarness()
		h.FS.File("/videos/a.mp4", tk.ETag("abc123"))
		_ = h.Scanner().ScanOnce(context.Background())
		tk.Equal(t, h.Queue.Pending()[0].SourceVersion, "abc123", "version")
	})
}

// Stage 2: reading state to decide what needs work.
func TestScannerFreshness(t *testing.T) {
	setup := func() *tk.Harness {
		h := tk.NewHarness()
		h.FS.File("/videos/a.mp4", tk.Size(10), tk.ModTimeMs(1)).File("/videos/b.mp4", tk.Size(20), tk.ModTimeMs(1))
		return h
	}
	ready := func(path, version string) domain.ThumbnailRecord {
		return domain.ThumbnailRecord{VideoPath: path, SourceVersion: version, Status: domain.StatusReady, Notified: true}
	}
	ctx := context.Background()

	t.Run("skips a video whose thumbnail is up to date", func(t *testing.T) {
		h := setup()
		_ = h.Repo.Upsert(ctx, ready("/videos/a.mp4", "10-1"))
		_ = h.Scanner().ScanOnce(ctx)
		tk.Equal(t, pending(h.Queue), []string{"/videos/b.mp4"}, "published")
		tk.Equal(t, h.Metrics.Get("scanner.skipped.up_to_date"), 1, "up to date")
	})

	t.Run("re-publishes a video whose source changed", func(t *testing.T) {
		h := setup()
		_ = h.Repo.Upsert(ctx, ready("/videos/a.mp4", "10-1"))
		h.FS.File("/videos/a.mp4", tk.Size(10), tk.ModTimeMs(999)) // touched
		_ = h.Scanner().ScanOnce(ctx)
		tk.Equal(t, pending(h.Queue), []string{"/videos/a.mp4", "/videos/b.mp4"}, "published")
	})

	t.Run("re-publishes a ready video whose event was never delivered", func(t *testing.T) {
		h := setup()
		rec := ready("/videos/a.mp4", "10-1")
		rec.Notified = false
		_ = h.Repo.Upsert(ctx, rec)
		_ = h.Scanner().ScanOnce(ctx)
		tk.Equal(t, pending(h.Queue), []string{"/videos/a.mp4", "/videos/b.mp4"}, "published")
	})

	t.Run("doesn't retry a permanently failed video until its source changes", func(t *testing.T) {
		h := setup()
		failed := ready("/videos/a.mp4", "10-1")
		failed.Status = domain.StatusFailed
		_ = h.Repo.Upsert(ctx, failed)

		_ = h.Scanner().ScanOnce(ctx)
		tk.Equal(t, pending(h.Queue), []string{"/videos/b.mp4"}, "first pass")

		h.FS.File("/videos/a.mp4", tk.Size(11), tk.ModTimeMs(2)) // re-uploaded
		_ = h.Scanner().ScanOnce(ctx)
		tk.True(t, slices.Contains(tk.PendingPaths(h.Queue), "/videos/a.mp4"), "changed video not re-published")
	})

	t.Run("fails open: publishes anyway when the repository is down", func(t *testing.T) {
		h := setup()
		h.Repo.FailAlways("Get")
		_ = h.Scanner().ScanOnce(ctx)
		tk.Equal(t, pending(h.Queue), []string{"/videos/a.mp4", "/videos/b.mp4"}, "published")
		tk.Equal(t, h.Metrics.Get("scanner.repository_errors"), 2, "repository errors")
	})
}

func TestScannerBackpressure(t *testing.T) {
	t.Run("pauses when the queue is full and resumes as it drains", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			h := tk.NewHarness(tk.WithQueueCapacity(5), tk.Configure(func(c *app.Config) { c.Scanner.WalkConcurrency = 2 }))
			for i := range 50 {
				h.FS.File(fmt.Sprintf("/videos/v%02d.mp4", i))
			}
			scanDone := make(chan error)
			go func() { scanDone <- h.Scanner().ScanOnce(context.Background()) }()
			synctest.Wait() // until the scanner is blocked

			tk.Equal(t, h.Queue.Stats().Ready, 5, "queued")
			_, opens := h.FS.Calls()
			tk.True(t, opens <= 5+2, "walker ran ahead of the queue: %d opens", opens)

			consumed := 0
			consumerDone := make(chan error)
			go func() {
				consumerDone <- h.Queue.Consume(context.Background(), 1, func(d queue.Delivery[domain.VideoJob]) error {
					consumed++
					return d.Ack()
				})
			}()
			tk.Equal(t, <-scanDone, nil, "scan error")
			h.Queue.Close()
			<-consumerDone
			tk.Equal(t, consumed, 50, "consumed")
		})
	})

	t.Run("stops promptly when cancelled while blocked on a full queue", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			h := tk.NewHarness(tk.WithQueueCapacity(2))
			for i := range 10 {
				h.FS.File(fmt.Sprintf("/videos/v%d.mp4", i))
			}
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error)
			go func() { done <- h.Scanner().ScanOnce(ctx) }()
			synctest.Wait()
			cancel()
			err := <-done
			tk.True(t, errors.Is(err, context.Canceled), "err = %v, want context.Canceled", err)
		})
	})
}

func TestScannerCheckpoints(t *testing.T) {
	tree := func(h *tk.Harness) {
		for i := range 6 {
			h.FS.File(fmt.Sprintf("/videos/v%d.mp4", i))
		}
		for i := range 4 {
			h.FS.File(fmt.Sprintf("/videos/sub/s%d.mp4", i))
		}
	}

	// crash runs a scan until it blocks on the full queue, then cancels it ("kills the process").
	crash := func(t *testing.T, h *tk.Harness) []string {
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error)
		go func() { done <- h.Scanner().ScanOnce(ctx) }()
		synctest.Wait()
		cancel()
		tk.True(t, errors.Is(<-done, context.Canceled), "expected the scan to be cancelled")
		return tk.PendingPaths(h.Queue)
	}

	t.Run("resumes after a crash with nothing missed or repeated (checkpoint every file)", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			h := tk.NewHarness(tk.WithQueueCapacity(4))
			tree(h)
			first := crash(t, h)
			tk.Equal(t, len(first), 4, "published before the crash")

			restarted := h.Restart()
			tk.Equal(t, restarted.Scanner.ScanOnce(context.Background()), nil, "resumed scan error")
			second := tk.PendingPaths(restarted.Queue)

			all := append(slices.Clone(first), second...)
			tk.Equal(t, len(all), 10, "total published")
			tk.Equal(t, len(slices.Compact(sorted(all))), 10, "unique published")
		})
	})

	t.Run("re-publishing after a crash is bounded by CheckpointEvery", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			h := tk.NewHarness(tk.WithQueueCapacity(17), tk.Configure(func(c *app.Config) { c.Scanner.CheckpointEvery = 5 }))
			for i := range 30 {
				h.FS.File(fmt.Sprintf("/videos/v%02d.mp4", i))
			}
			first := crash(t, h)

			restarted := h.Restart()
			_ = restarted.Scanner.ScanOnce(context.Background())
			second := tk.PendingPaths(restarted.Queue)

			dupes := 0
			for _, p := range second {
				if slices.Contains(first, p) {
					dupes++
				}
			}
			tk.Equal(t, len(slices.Compact(sorted(append(first, second...)))), 30, "nothing missing")
			tk.True(t, dupes < 5, "%d files re-published, want < CheckpointEvery", dupes)
		})
	})

	t.Run("clears the checkpoint after a completed pass", func(t *testing.T) {
		h := tk.NewHarness()
		tree(h)
		_ = h.Scanner().ScanOnce(context.Background())
		cp, _ := h.Checkpoints.Load(context.Background())
		tk.True(t, cp == nil, "checkpoint not cleared: %+v", cp)
		tk.Equal(t, h.Metrics.Get("scanner.passes_completed"), 1, "passes")
	})

	t.Run("ignores a checkpoint for a different root", func(t *testing.T) {
		h := tk.NewHarness()
		tree(h)
		_ = h.Checkpoints.Save(context.Background(), scanner.Checkpoint{Root: "/other"})
		_ = h.Scanner().ScanOnce(context.Background())
		tk.Equal(t, len(h.Queue.Pending()), 10, "published")
	})

	t.Run("keeps scanning when checkpoints can't be loaded or saved", func(t *testing.T) {
		h := tk.NewHarness()
		tree(h)
		h.Checkpoints.FailAlways("Load")
		h.Checkpoints.FailAlways("Save")
		_ = h.Scanner().ScanOnce(context.Background())
		tk.Equal(t, len(h.Queue.Pending()), 10, "published")
		tk.Equal(t, h.Metrics.Get("scanner.checkpoint_errors"), 10, "checkpoint errors")
	})
}
