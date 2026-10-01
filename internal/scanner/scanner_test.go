package scanner_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"pgregory.net/rapid"

	"github.com/vukkremic/thumbnailer/internal/app"
	"github.com/vukkremic/thumbnailer/internal/domain"
	"github.com/vukkremic/thumbnailer/internal/queue"
	tk "github.com/vukkremic/thumbnailer/internal/testkit"
)

func sorted(s []string) []string {
	out := slices.Clone(s)
	slices.Sort(out)
	return out
}

func published(q *queue.Memory[domain.VideoJob]) []string { return sorted(tk.PendingPaths(q)) }

func scan(t *testing.T, h *tk.Harness) {
	t.Helper()
	tk.Equal(t, h.Scanner().ScanOnce(context.Background()), nil, "ScanOnce error")
}

func TestScannerTraversal(t *testing.T) {
	t.Run("an empty root publishes nothing and completes", func(t *testing.T) {
		h := tk.NewHarness()
		scan(t, h)
		tk.Equal(t, len(h.Queue.Pending()), 0, "published")
		tk.Equal(t, h.Metrics.Get("scanner.passes_completed"), 1, "passes")
	})

	t.Run("publishes every video in a nested tree", func(t *testing.T) {
		h := tk.NewHarness()
		h.FS.File("/videos/a.mp4").File("/videos/sub/b.mov").File("/videos/sub/deeper/c.mkv").Dir("/videos/emptyDir")
		scan(t, h)
		tk.Equal(t, published(h.Queue), []string{"/videos/a.mp4", "/videos/sub/b.mov", "/videos/sub/deeper/c.mkv"}, "published")
		tk.Equal(t, h.Dirs.Stats(), queue.Stats{}, "dir queue drained")
	})

	t.Run("very deep nesting", func(t *testing.T) {
		h := tk.NewHarness()
		deep := "/videos/" + strings.Repeat("d/", 3000) + "bottom.mp4"
		h.FS.File(deep)
		scan(t, h)
		tk.Equal(t, published(h.Queue), []string{deep}, "published")
	})

	t.Run("a huge directory is split into pages", func(t *testing.T) {
		h := tk.NewHarness(tk.WithQueueCapacity(5000))
		h.FS.WithPageSize(1000)
		for i := range 2500 {
			h.FS.File(fmt.Sprintf("/videos/v%04d.mp4", i))
		}
		scan(t, h)
		tk.Equal(t, len(published(h.Queue)), 2500, "published")
		tk.Equal(t, len(slices.Compact(published(h.Queue))), 2500, "unique")
		tk.Equal(t, h.FS.ListCalls("/videos"), 3, "pages listed")
	})

	t.Run("listing returns metadata, so no file is stat'ed", func(t *testing.T) {
		h := tk.NewHarness()
		for i := range 50 {
			h.FS.File(fmt.Sprintf("/videos/d%d/v.mp4", i))
		}
		scan(t, h)
		tk.Equal(t, h.FS.OpenCalls(), 0, "OpenFile calls")
	})

	t.Run("symlinks are skipped", func(t *testing.T) {
		h := tk.NewHarness()
		h.FS.File("/videos/a.mp4").Symlink("/videos/loop").Symlink("/videos/link.mp4")
		scan(t, h)
		tk.Equal(t, published(h.Queue), []string{"/videos/a.mp4"}, "published")
		tk.Equal(t, h.Metrics.Get("scanner.skipped.symlink"), 2, "symlinks skipped")
	})
}

func TestScannerParallelListing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// root → 8 dirs → 64 dirs, every listing takes 20ms.
		h := tk.NewHarness(tk.Configure(func(c *app.Config) { c.Scanner.Concurrency = 8 }))
		h.FS.WithLatency(20 * time.Millisecond)
		for i := range 8 {
			for j := range 8 {
				h.FS.File(fmt.Sprintf("/videos/a%d/b%d/v.mp4", i, j))
			}
		}
		start := time.Now()
		scan(t, h)

		tk.Equal(t, len(h.Queue.Pending()), 64, "published")
		tk.Equal(t, h.FS.PeakInFlight(), 8, "peak concurrent listings")
		// Serially: 73 listings × 20ms = 1.46s. In parallel: root, then 8 at once, then 64 in
		// batches of 8 = 10 rounds.
		tk.Equal(t, time.Since(start), 10*20*time.Millisecond, "elapsed")
	})
}

func TestScannerPublishing(t *testing.T) {
	t.Run("publishes readable, non-empty videos and counts the skips", func(t *testing.T) {
		h := tk.NewHarness()
		h.FS.File("/videos/a.mp4", tk.Size(10), tk.ModTimeMs(5)).
			File("/videos/notes.txt").
			File("/videos/empty.mp4", tk.Size(0)).
			File("/videos/locked.mov", tk.Perm(0)).
			File("/videos/sub/B.MKV")
		scan(t, h)

		tk.Equal(t, published(h.Queue), []string{"/videos/a.mp4", "/videos/sub/B.MKV"}, "published")
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
		scan(t, h)
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
		scan(t, h)
		tk.Equal(t, published(h.Queue), []string{"/videos/b.mp4"}, "published")
		tk.Equal(t, h.Metrics.Get("scanner.skipped.up_to_date"), 1, "up to date")
	})

	t.Run("re-publishes a video whose source changed", func(t *testing.T) {
		h := setup()
		_ = h.Repo.Upsert(ctx, ready("/videos/a.mp4", "10-1"))
		h.FS.File("/videos/a.mp4", tk.Size(10), tk.ModTimeMs(999)) // touched
		scan(t, h)
		tk.Equal(t, published(h.Queue), []string{"/videos/a.mp4", "/videos/b.mp4"}, "published")
	})

	t.Run("re-publishes a ready video whose event was never delivered", func(t *testing.T) {
		h := setup()
		rec := ready("/videos/a.mp4", "10-1")
		rec.Notified = false
		_ = h.Repo.Upsert(ctx, rec)
		scan(t, h)
		tk.Equal(t, published(h.Queue), []string{"/videos/a.mp4", "/videos/b.mp4"}, "published")
	})

	t.Run("doesn't retry a permanently failed video until its source changes", func(t *testing.T) {
		h := setup()
		failed := ready("/videos/a.mp4", "10-1")
		failed.Status = domain.StatusFailed
		_ = h.Repo.Upsert(ctx, failed)

		scan(t, h)
		tk.Equal(t, published(h.Queue), []string{"/videos/b.mp4"}, "first pass")

		h.FS.File("/videos/a.mp4", tk.Size(11), tk.ModTimeMs(2)) // re-uploaded
		scan(t, h)
		tk.True(t, slices.Contains(tk.PendingPaths(h.Queue), "/videos/a.mp4"), "changed video not re-published")
	})

	t.Run("one batched lookup per page, never one per file", func(t *testing.T) {
		h := tk.NewHarness()
		h.FS.WithPageSize(100)
		for i := range 250 {
			h.FS.File(fmt.Sprintf("/videos/v%03d.mp4", i))
		}
		scan(t, h)
		tk.Equal(t, h.Repo.Calls("GetMany"), 3, "GetMany calls (3 pages)")
		tk.Equal(t, h.Repo.Calls("Get"), 0, "Get calls")
	})

	t.Run("fails open: publishes anyway when the repository is down", func(t *testing.T) {
		h := setup()
		h.Repo.FailAlways("GetMany")
		scan(t, h)
		tk.Equal(t, published(h.Queue), []string{"/videos/a.mp4", "/videos/b.mp4"}, "published")
		tk.Equal(t, h.Metrics.Get("scanner.repository_errors"), 1, "repository errors")
	})
}

func TestScannerListFailures(t *testing.T) {
	t.Run("a transient listing failure is retried and the pass still completes", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			h := tk.NewHarness()
			h.FS.File("/videos/flaky/a.mp4").File("/videos/ok/b.mp4").FailListTimes("/videos/flaky", 2, nil)
			scan(t, h)
			tk.Equal(t, published(h.Queue), []string{"/videos/flaky/a.mp4", "/videos/ok/b.mp4"}, "published")
			tk.Equal(t, h.Metrics.Get("scanner.list_retries"), 2, "retries")
		})
	})

	t.Run("a directory that keeps failing is dead-lettered; the rest continues", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			h := tk.NewHarness()
			h.FS.File("/videos/locked/secret.mp4").File("/videos/open/ok.mp4").FailList("/videos/locked", nil)
			scan(t, h)
			tk.Equal(t, published(h.Queue), []string{"/videos/open/ok.mp4"}, "published")
			tk.Equal(t, h.FS.ListCalls("/videos/locked"), 3, "attempts (MaxAttempts)")
			tk.Equal(t, len(h.Dirs.DeadLetters()), 1, "dead-lettered directories")
			tk.Equal(t, h.Metrics.Get("scanner.list_errors"), 1, "list errors")
		})
	})

	t.Run("a directory deleted before it's listed is skipped quietly", func(t *testing.T) {
		h := tk.NewHarness()
		h.FS.File("/videos/gone/a.mp4").File("/videos/here.mp4").FailList("/videos/gone", tk.NotExist("/videos/gone"))
		scan(t, h)
		tk.Equal(t, published(h.Queue), []string{"/videos/here.mp4"}, "published")
		tk.Equal(t, h.Metrics.Get("scanner.vanished"), 1, "vanished")
		tk.Equal(t, h.Metrics.Get("scanner.list_retries"), 0, "retries")
	})

	t.Run("a missing root completes with nothing published", func(t *testing.T) {
		h := tk.NewHarness(tk.Configure(func(c *app.Config) { c.Scanner.Root = "/nope" }))
		scan(t, h)
		tk.Equal(t, len(h.Queue.Pending()), 0, "published")
	})

	t.Run("property: any tree, any transient failures: every video published, pass completes", func(t *testing.T) {
		rapid.Check(t, func(rt *rapid.T) {
			h := tk.NewHarness(tk.Configure(func(c *app.Config) {
				c.Scanner.MaxAttempts = 3
				c.Scanner.RetryDelay = 0
				c.Scanner.Concurrency = rapid.IntRange(1, 6).Draw(rt, "concurrency")
			}))
			h.FS.WithPageSize(rapid.IntRange(1, 5).Draw(rt, "pageSize"))
			videos := genTree(rt, h.FS, "/videos", 0)

			if err := h.Scanner().ScanOnce(context.Background()); err != nil {
				rt.Fatalf("ScanOnce: %v", err)
			}
			got := slices.Compact(published(h.Queue))
			if !slices.Equal(got, sorted(videos)) {
				rt.Fatalf("published %v, want %v", got, sorted(videos))
			}
			if s := h.Dirs.Stats(); s != (queue.Stats{}) {
				rt.Fatalf("dir queue not drained: %+v", s)
			}
		})
	})
}

func TestScannerBackpressure(t *testing.T) {
	t.Run("listing pauses while the video queue is full, and resumes as it drains", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			h := tk.NewHarness(tk.WithQueueCapacity(5), tk.Configure(func(c *app.Config) { c.Scanner.Concurrency = 2 }))
			h.FS.WithPageSize(10)
			for i := range 20 {
				for j := range 10 {
					h.FS.File(fmt.Sprintf("/videos/d%02d/v%d.mp4", i, j))
				}
			}
			scanDone := make(chan error)
			go func() { scanDone <- h.Scanner().ScanOnce(context.Background()) }()
			synctest.Wait()

			tk.Equal(t, h.Queue.Stats().Ready, 5, "queued")
			// Bounded by scanner concurrency (2), not by the 21 directories in the tree.
			tk.True(t, h.FS.ListCalls() <= 1+2*2, "listing ran ahead of the queue: %d listings", h.FS.ListCalls())

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
			tk.Equal(t, consumed, 200, "consumed")
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

func genTree(rt *rapid.T, fs *tk.FS, dir string, depth int) []string {
	var videos []string
	names := rapid.SliceOfNDistinct(rapid.StringMatching(`[a-z]{1,3}`), 0, 4, rapid.ID[string]).Draw(rt, "names")
	for _, name := range names {
		p := dir + "/" + name
		if depth < 3 && rapid.Bool().Draw(rt, "isDir") {
			fs.Dir(p)
			if failures := rapid.IntRange(0, 2).Draw(rt, "failures"); failures > 0 {
				fs.FailListTimes(p, failures, nil) // transient: under MaxAttempts
			}
			videos = append(videos, genTree(rt, fs, p, depth+1)...)
		} else {
			fs.File(p + ".mp4")
			videos = append(videos, p+".mp4")
		}
	}
	return videos
}
