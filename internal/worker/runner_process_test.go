package worker_test

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/vukkremic/thumbnailer/internal/app"
	"github.com/vukkremic/thumbnailer/internal/domain"
	tk "github.com/vukkremic/thumbnailer/internal/testkit"
	"github.com/vukkremic/thumbnailer/internal/worker"
)

const video = "/videos/clip.mp4"

// fixture is a harness with one video on disk, plus a helper that delivers its job to the runner.
type fixture struct {
	*tk.Harness
	runner *worker.Runner
}

func newFixture(opts ...tk.Option) *fixture {
	h := tk.NewHarness(opts...)
	h.FS.File(video, tk.Size(5000), tk.ModTimeMs(10))
	return &fixture{Harness: h, runner: h.Runner()}
}

func (f *fixture) job() domain.VideoJob {
	info, _ := f.FS.OpenFile(context.Background(), video)
	return domain.JobFor(info)
}

// deliver hands the job to the runner as attempt `count` and returns the outcome and settlement.
func (f *fixture) deliver(count int) (worker.Outcome, tk.Settlement) {
	return f.deliverJob(context.Background(), f.job(), count)
}

func (f *fixture) deliverJob(ctx context.Context, job domain.VideoJob, count int) (worker.Outcome, tk.Settlement) {
	d := &tk.RecordingDelivery{Job: job, Count: count}
	outcome := f.runner.Handle(ctx, d)
	if d.Settled == nil {
		panic("runner returned without settling the delivery")
	}
	return outcome, *d.Settled
}

func (f *fixture) record() (domain.ThumbnailRecord, bool) {
	rec, ok, _ := f.Repo.MemoryRepository.Get(context.Background(), video)
	return rec, ok
}

// noLeftovers checks the resource invariants every scenario must hold.
func (f *fixture) noLeftovers(t *testing.T) {
	t.Helper()
	tk.Equal(t, len(f.Scratch.Files()), 0, "scratch files left behind")
	tk.Equal(t, len(f.Launcher.Leaked()), 0, "leaked processes")
}

// Stage 3: running ffmpeg, one group per failure state.
func TestRunnerSuccess(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFixture()
		outcome, s := f.deliver(1)

		tk.Equal(t, outcome, worker.Generated, "outcome")
		tk.Equal(t, s, tk.Settlement{Type: "ack"}, "settlement")
		rec, _ := f.record()
		tk.Equal(t, [4]any{rec.Status, rec.SourceVersion, rec.ContentType, rec.Attempts},
			[4]any{domain.StatusReady, "5000-10", "image/jpeg", 1}, "record")
		tk.True(t, bytes.Equal(f.Blobs.Objects()[rec.BlobKey].Data, tk.JPEG), "stored blob differs")
		f.noLeftovers(t)
	})
}

func TestRunnerProcessError(t *testing.T) {
	t.Run("a transient failure is retried with backoff", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			f := newFixture()
			f.Launcher.Script(video, tk.ExitWith(1, tk.TransientStderr))
			outcome, s := f.deliver(1)

			tk.Equal(t, outcome, worker.RetryScheduled, "outcome")
			tk.Equal(t, s.Type, "nack", "settlement")
			tk.True(t, s.Delay >= 500*time.Millisecond && s.Delay <= time.Second, "attempt-1 delay %s outside [500ms, 1s]", s.Delay)
			_, found := f.record()
			tk.Equal(t, found, false, "record written for a transient failure")
			f.noLeftovers(t)
		})
	})

	t.Run("each attempt waits longer", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			f := newFixture(tk.Configure(func(c *app.Config) { c.Runner.MaxAttempts = 10 }))
			f.Launcher.Script(video, tk.ExitWith(1, tk.TransientStderr))
			var delays []time.Duration
			for attempt := 1; attempt <= 4; attempt++ {
				_, s := f.deliver(attempt)
				delays = append(delays, s.Delay)
			}
			// Ranges [0.5,1], [1,2], [2,4], [4,8]s don't overlap, so this is strictly increasing.
			for i := 1; i < len(delays); i++ {
				tk.True(t, delays[i] >= delays[i-1], "delays not increasing: %v", delays)
			}
			tk.True(t, delays[3] >= 4*time.Second, "attempt-4 delay %s", delays[3])
		})
	})

	t.Run("after MaxAttempts: dead-lettered with the last stderr, failure recorded", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			f := newFixture()
			f.Launcher.Script(video, tk.ExitWith(1, tk.TransientStderr))
			outcome, s := f.deliver(3)

			tk.Equal(t, outcome, worker.DeadLettered, "outcome")
			tk.Equal(t, s, tk.Settlement{Type: "dead_letter", Reason: "process_error: " + tk.TransientStderr}, "settlement")
			rec, _ := f.record()
			tk.Equal(t, [4]any{rec.Status, rec.Attempts, rec.FailureKind, rec.FailureReason},
				[4]any{domain.StatusFailed, 3, "process_error", tk.TransientStderr}, "record")
		})
	})

	t.Run("a launcher that fails to start ffmpeg is retryable", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			f := newFixture()
			f.Launcher.Script(video, tk.SpawnError())
			outcome, _ := f.deliver(1)
			tk.Equal(t, outcome, worker.RetryScheduled, "outcome")
			tk.Equal(t, f.Metrics.Get("worker.failures.spawn_error"), 1, "metric")
			f.noLeftovers(t)
		})
	})
}

func TestRunnerTimeout(t *testing.T) {
	t.Run("a hung process is killed at exactly the timeout, retried, nothing leaks", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			f := newFixture()
			f.Launcher.Script(video, tk.Hang(0))
			start := time.Now()
			outcome, _ := f.deliver(1)

			tk.Equal(t, outcome, worker.RetryScheduled, "outcome")
			tk.Equal(t, time.Since(start), time.Minute, "elapsed")
			tk.Equal(t, f.Launcher.Processes()[0].State, tk.Killed, "state")
			tk.Equal(t, f.Metrics.Get("worker.timeouts"), 1, "timeouts")
			f.noLeftovers(t)
		})
	})

	t.Run("escalates the kill when SIGTERM is ignored", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			f := newFixture()
			f.Launcher.Script(video, tk.Hang(1))
			f.deliver(1)
			tk.Equal(t, f.Launcher.Processes()[0], tk.ProcSnapshot{VideoPath: video, State: tk.Killed, Kills: 2}, "process")
			f.noLeftovers(t)
		})
	})

	t.Run("an unkillable process frees the slot anyway and raises an alert", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			f := newFixture()
			f.Launcher.Script(video, tk.Hang(-1))
			outcome, _ := f.deliver(1)
			tk.Equal(t, outcome, worker.RetryScheduled, "outcome")
			tk.Equal(t, f.Metrics.Get("worker.processes_unreaped"), 1, "unreaped")
			tk.True(t, f.Logs.Has(slog.LevelError, "ignored kill"), "no error log")
			f.Launcher.ForceExitAll()
		})
	})

	t.Run("succeeds on a later attempt after a timeout", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			f := newFixture()
			f.Launcher.Script(video, tk.Hang(0), tk.Succeed())
			o1, _ := f.deliver(1)
			o2, _ := f.deliver(2)
			tk.Equal(t, []worker.Outcome{o1, o2}, []worker.Outcome{worker.RetryScheduled, worker.Generated}, "outcomes")
		})
	})
}

func TestRunnerSizeLimits(t *testing.T) {
	t.Run("an oversized input is rejected before spawning ffmpeg", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			f := newFixture(tk.Configure(func(c *app.Config) { c.Runner.MaxInputBytes = 4999 }))
			outcome, s := f.deliver(1)
			tk.Equal(t, outcome, worker.DeadLettered, "outcome")
			tk.True(t, strings.HasPrefix(s.Reason, "input_too_large"), "reason %q", s.Reason)
			tk.Equal(t, len(f.Launcher.Processes()), 0, "processes spawned")
			rec, _ := f.record()
			tk.Equal(t, rec.FailureKind, "input_too_large", "recorded failure")
		})
	})

	t.Run("an input exactly at the limit is accepted", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			f := newFixture(tk.Configure(func(c *app.Config) { c.Runner.MaxInputBytes = 5000 }))
			outcome, _ := f.deliver(1)
			tk.Equal(t, outcome, worker.Generated, "outcome")
		})
	})

	t.Run("an oversized output is rejected permanently and nothing is stored", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			f := newFixture(tk.Configure(func(c *app.Config) { c.Runner.MaxOutputBytes = 8 }))
			outcome, _ := f.deliver(1)
			tk.Equal(t, outcome, worker.DeadLettered, "outcome")
			tk.Equal(t, f.Metrics.Get("worker.failures.output_too_large"), 1, "metric")
			tk.Equal(t, len(f.Blobs.Objects()), 0, "blobs")
			f.noLeftovers(t)
		})
	})
}

func TestRunnerCorruptData(t *testing.T) {
	t.Run("corrupt input is dead-lettered on the first attempt, no retries", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			f := newFixture()
			f.Launcher.Script(video, tk.ExitWith(1, tk.CorruptStderr))
			outcome, s := f.deliver(1)
			tk.Equal(t, outcome, worker.DeadLettered, "outcome")
			tk.True(t, strings.HasPrefix(s.Reason, "corrupt_input"), "reason %q", s.Reason)
			tk.Equal(t, f.Metrics.Get("worker.retries"), 0, "retries")
		})
	})

	outputs := map[string][]byte{
		"no output file":           nil,
		"an empty output file":     {},
		"a non-image output file":  []byte("this is not a jpeg"),
		"a truncated image header": {0xff, 0xd8},
	}
	for name, output := range outputs {
		t.Run("exit 0 with "+name+" is not trusted", func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newFixture()
				f.Launcher.Script(video, tk.SucceedWithOutput(output))
				outcome, _ := f.deliver(1)
				tk.Equal(t, outcome, worker.RetryScheduled, "outcome")
				tk.Equal(t, f.Metrics.Get("worker.failures.invalid_output"), 1, "metric")
				tk.Equal(t, len(f.Blobs.Objects()), 0, "blobs")
				f.noLeftovers(t)
			})
		})
	}

	t.Run("invalid output is dead-lettered once attempts run out", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			f := newFixture()
			f.Launcher.Script(video, tk.SucceedWithOutput(nil))
			outcome, _ := f.deliver(3)
			tk.Equal(t, outcome, worker.DeadLettered, "outcome")
		})
	})
}

func TestRunnerStaleJobs(t *testing.T) {
	t.Run("source changed after publishing: ack, no work", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			f := newFixture()
			job := f.job()
			f.FS.File(video, tk.Size(6000), tk.ModTimeMs(99))
			outcome, s := f.deliverJob(context.Background(), job, 1)
			tk.Equal(t, outcome, worker.Superseded, "outcome")
			tk.Equal(t, s.Type, "ack", "settlement")
			tk.Equal(t, len(f.Launcher.Processes()), 0, "processes")
		})
	})

	t.Run("source deleted: ack, no work", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			f := newFixture()
			job := f.job()
			f.FS.Remove(video)
			outcome, _ := f.deliverJob(context.Background(), job, 1)
			tk.Equal(t, outcome, worker.Superseded, "outcome")
			tk.Equal(t, f.Metrics.Get("worker.superseded.gone"), 1, "metric")
		})
	})

	t.Run("source can't be checked: retry", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			f := newFixture()
			job := f.job()
			f.FS.FailOpen(video, nil)
			outcome, _ := f.deliverJob(context.Background(), job, 1)
			tk.Equal(t, outcome, worker.RetryScheduled, "outcome")
			tk.Equal(t, len(f.Launcher.Processes()), 0, "processes")
		})
	})
}

func TestRunnerShutdown(t *testing.T) {
	t.Run("shutdown already started: job released untouched", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			f := newFixture()
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			outcome, s := f.deliverJob(ctx, f.job(), 1)
			tk.Equal(t, outcome, worker.Released, "outcome")
			tk.Equal(t, s, tk.Settlement{Type: "nack"}, "settlement (immediate redelivery)")
			tk.Equal(t, len(f.Launcher.Processes()), 0, "processes")
		})
	})

	t.Run("shutdown mid-process: ffmpeg killed, job returned, nothing written", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			f := newFixture()
			f.Launcher.Script(video, tk.SucceedAfter(30*time.Second))
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			outcome, s := f.deliverJob(ctx, f.job(), 1)
			tk.Equal(t, outcome, worker.Released, "outcome")
			tk.Equal(t, s, tk.Settlement{Type: "nack"}, "settlement")
			tk.Equal(t, f.Launcher.Processes()[0].State, tk.Killed, "state")
			_, found := f.record()
			tk.Equal(t, found, false, "record written")
			f.noLeftovers(t)
		})
	})
}
