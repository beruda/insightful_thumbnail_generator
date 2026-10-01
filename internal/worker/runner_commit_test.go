package worker_test

import (
	"errors"
	"strings"
	"testing"
	"testing/synctest"

	"github.com/vukkremic/thumbnailer/internal/app"
	"github.com/vukkremic/thumbnailer/internal/domain"
	tk "github.com/vukkremic/thumbnailer/internal/testkit"
	"github.com/vukkremic/thumbnailer/internal/worker"
)

// Stage 4: writing the thumbnail (blob + metadata).
func TestRunnerWrite(t *testing.T) {
	t.Run("a failed blob write is retried; no record, no partial object", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			f := newFixture()
			f.Blobs.FailNext("Put", 1)

			outcome, _ := f.deliver(1)
			tk.Equal(t, outcome, worker.RetryScheduled, "first attempt")
			tk.Equal(t, len(f.Blobs.Objects()), 0, "blobs")
			_, found := f.record()
			tk.Equal(t, found, false, "record")
			f.noLeftovers(t)

			outcome, _ = f.deliver(2)
			tk.Equal(t, outcome, worker.Generated, "second attempt")
			tk.Equal(t, len(f.Blobs.Objects()), 1, "blobs")
		})
	})

	t.Run("blob written but record not: the retry is idempotent (same key, one object)", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			f := newFixture()
			f.Repo.FailNext("Upsert", 1)

			outcome, _ := f.deliver(1)
			tk.Equal(t, outcome, worker.RetryScheduled, "first attempt")
			tk.Equal(t, len(f.Blobs.Objects()), 1, "orphan blob (harmless: nothing points at it)")

			outcome, _ = f.deliver(2)
			tk.Equal(t, outcome, worker.Generated, "second attempt")
			tk.Equal(t, len(f.Blobs.Objects()), 1, "objects (overwritten, not duplicated)")
			tk.Equal(t, f.Blobs.Puts(), 2, "puts")
			rec, _ := f.record()
			tk.Equal(t, [2]any{rec.Status, rec.Attempts}, [2]any{domain.StatusReady, 2}, "record")
		})
	})

	t.Run("dead-lettered if storage stays down", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			f := newFixture()
			f.Blobs.FailAlways("Put")
			outcome, s := f.deliver(3)
			tk.Equal(t, outcome, worker.DeadLettered, "outcome")
			tk.True(t, strings.HasPrefix(s.Reason, "storage_error"), "reason %q", s.Reason)
		})
	})

	t.Run("duplicate deliveries produce a single result", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			f := newFixture()
			first, _ := f.deliver(1)
			second, s := f.deliver(1)
			tk.Equal(t, []worker.Outcome{first, second}, []worker.Outcome{worker.Generated, worker.AlreadySettled}, "outcomes")
			tk.Equal(t, s.Type, "ack", "duplicate settlement")
			tk.Equal(t, len(f.Launcher.Processes()), 1, "ffmpeg runs")
			tk.Equal(t, f.Blobs.Puts(), 1, "puts")
		})
	})

	t.Run("an unreadable repository nacks the job instead of dropping it", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			f := newFixture()
			f.Repo.FailNext("Get", 1)
			outcome, s := f.deliver(1)
			tk.Equal(t, outcome, worker.RetryScheduled, "outcome")
			tk.Equal(t, s.Type, "nack", "settlement")
			tk.Equal(t, len(f.Launcher.Processes()), 0, "processes")
		})
	})

	t.Run("a permanent failure is dead-lettered even if recording it fails", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			f := newFixture(tk.Configure(func(c *app.Config) { c.Runner.MaxInputBytes = 1 }))
			f.Repo.FailAlways("Upsert")
			_, s := f.deliver(1)
			tk.Equal(t, s.Type, "dead_letter", "settlement")
		})
	})

	t.Run("regression: a ready thumbnail is never downgraded to failed", func(t *testing.T) {
		// Found by the chaos test: a duplicate delivery failing on its last attempt overwrote the
		// ready record written by the other delivery. Here the duplicate's first read fails (as if
		// it raced the other delivery), sending it down the failure path on its final attempt.
		synctest.Test(t, func(t *testing.T) {
			f := newFixture()
			f.deliver(1) // the other delivery succeeds
			f.Repo.FailNext("Get", 1)
			outcome, s := f.deliver(3)
			tk.Equal(t, outcome, worker.AlreadySettled, "outcome")
			tk.Equal(t, s.Type, "ack", "settlement")
			rec, _ := f.record()
			tk.Equal(t, rec.Status, domain.StatusReady, "status")
		})
	})
}

// Stage 5: the "done" signal (ack + ReadyEvent).
func TestRunnerDoneSignal(t *testing.T) {
	t.Run("exactly one ReadyEvent per generated thumbnail", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			f := newFixture()
			f.deliver(1)
			f.deliver(1) // duplicate delivery
			rec, _ := f.record()
			tk.Equal(t, f.Events.Published(), []domain.ReadyEvent{{
				Type: "thumbnail.ready", VideoPath: video, SourceVersion: "5000-10", BlobKey: rec.BlobKey, ContentType: "image/jpeg",
			}}, "events")
			tk.Equal(t, rec.Notified, true, "notified")
		})
	})

	t.Run("no ack until blob and record are committed", func(t *testing.T) {
		for _, fail := range []func(f *fixture){
			func(f *fixture) { f.Blobs.FailNext("Put", 1) },
			func(f *fixture) { f.Repo.FailNext("Upsert", 1) },
		} {
			synctest.Test(t, func(t *testing.T) {
				f := newFixture()
				fail(f)
				_, s := f.deliver(1)
				tk.Equal(t, s.Type, "nack", "settlement")
				tk.Equal(t, len(f.Events.Published()), 0, "events")
			})
		}
	})

	t.Run("a failed event is retried without re-running ffmpeg", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			f := newFixture()
			f.Events.FailNext("Publish", 1)

			outcome, s := f.deliver(1)
			tk.Equal(t, outcome, worker.RetryScheduled, "first attempt")
			tk.Equal(t, s.Type, "nack", "settlement")
			rec, _ := f.record()
			tk.Equal(t, [2]any{rec.Status, rec.Notified}, [2]any{domain.StatusReady, false}, "record after first attempt")

			outcome, s = f.deliver(2)
			tk.Equal(t, outcome, worker.AlreadySettled, "retry")
			tk.Equal(t, s.Type, "ack", "retry settlement")
			tk.Equal(t, len(f.Events.Published()), 1, "events")
			tk.Equal(t, len(f.Launcher.Processes()), 1, "ffmpeg runs")
		})
	})

	t.Run("event bus down past MaxAttempts: thumbnail stays ready, job acked, deferred to next scan", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			f := newFixture()
			f.Events.FailAlways("Publish")
			outcome, s := f.deliver(3)
			tk.Equal(t, outcome, worker.NotifyDeferred, "outcome")
			tk.Equal(t, s.Type, "ack", "settlement (never dead-lettered: the thumbnail exists)")
			rec, _ := f.record()
			tk.Equal(t, [2]any{rec.Status, rec.Notified}, [2]any{domain.StatusReady, false}, "record")
			tk.Equal(t, domain.NeedsWork(rec, true, rec.SourceVersion), true, "the next scan picks it up")
		})
	})

	t.Run("at-least-once if the crash lands between publishing and marking notified", func(t *testing.T) {
		// Documents the known limit: closing it fully needs a transactional outbox.
		// Consumers dedupe on (videoPath, sourceVersion).
		synctest.Test(t, func(t *testing.T) {
			f := newFixture()
			upserts := 0
			f.Repo.BeforeUpsert = func(domain.ThumbnailRecord) error {
				if upserts++; upserts == 2 { // 1st: ready record, 2nd: notified flag
					return errors.New("crash after publish")
				}
				return nil
			}
			f.deliver(1)
			f.deliver(2)
			events := f.Events.Published()
			tk.Equal(t, len(events), 2, "events")
			tk.Equal(t, events[0], events[1], "duplicates are identical, so consumers can dedupe")
		})
	})

	t.Run("no event for failures", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			f := newFixture(tk.Configure(func(c *app.Config) { c.Runner.MaxInputBytes = 1 }))
			f.deliver(1)
			tk.Equal(t, len(f.Events.Published()), 0, "events")
		})
	})
}
