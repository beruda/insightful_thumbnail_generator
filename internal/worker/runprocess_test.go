package worker_test

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	tk "github.com/vukkremic/thumbnailer/internal/testkit"
	"github.com/vukkremic/thumbnailer/internal/worker"
)

const (
	timeout   = time.Second
	killGrace = 100 * time.Millisecond
)

func launch(t *testing.T, l *tk.Launcher) (*tk.Launcher, func(context.Context) worker.ProcessResult) {
	p, err := l.Launch("/v.mp4", "/out.jpg")
	tk.Equal(t, err, nil, "launch")
	return l, func(ctx context.Context) worker.ProcessResult { return worker.RunProcess(ctx, p, timeout, killGrace) }
}

func TestRunProcess(t *testing.T) {
	t.Run("returns exit code and stderr of a process that finishes in time", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			_, run := launch(t, tk.NewLauncher(tk.NewScratch()).Script("/v.mp4", tk.ExitWith(1, "boom")))
			start := time.Now()
			got := run(context.Background())
			tk.Equal(t, got, worker.ProcessResult{Outcome: worker.Exited, ExitCode: 1, Stderr: "boom", Reaped: true}, "result")
			tk.Equal(t, time.Since(start), 10*time.Millisecond, "elapsed")
		})
	})

	t.Run("kills a process that exceeds the timeout, and confirms it exited", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			l, run := launch(t, tk.NewLauncher(tk.NewScratch()).Script("/v.mp4", tk.Hang(0)))
			start := time.Now()
			got := run(context.Background())
			tk.Equal(t, got.Outcome, worker.TimedOut, "outcome")
			tk.Equal(t, got.Reaped, true, "reaped")
			tk.Equal(t, time.Since(start), timeout, "elapsed")
			tk.Equal(t, l.Processes()[0], tk.ProcSnapshot{VideoPath: "/v.mp4", State: tk.Killed, Kills: 1}, "process")
		})
	})

	t.Run("escalates when the first kill is ignored (SIGTERM → SIGKILL)", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			l, run := launch(t, tk.NewLauncher(tk.NewScratch()).Script("/v.mp4", tk.Hang(1)))
			start := time.Now()
			got := run(context.Background())
			tk.Equal(t, got.Reaped, true, "reaped")
			tk.Equal(t, l.Processes()[0].Kills, 2, "kills")
			tk.Equal(t, time.Since(start), timeout+killGrace, "elapsed (timeout + one grace period)")
		})
	})

	t.Run("gives up on an unkillable process instead of blocking forever, and reports it", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			l, run := launch(t, tk.NewLauncher(tk.NewScratch()).Script("/v.mp4", tk.Hang(-1)))
			start := time.Now()
			got := run(context.Background())
			tk.Equal(t, got.Outcome, worker.TimedOut, "outcome")
			tk.Equal(t, got.Reaped, false, "reaped")
			tk.Equal(t, time.Since(start), timeout+2*killGrace, "elapsed (bounded)")
			l.ForceExitAll()
		})
	})

	t.Run("kills the process when the context is cancelled (shutdown)", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			l, run := launch(t, tk.NewLauncher(tk.NewScratch()).Script("/v.mp4", tk.SucceedAfter(10*time.Second)))
			ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
			defer cancel()
			got := worker.ProcessResult{}
			start := time.Now()
			got = run(ctx)
			tk.Equal(t, got.Outcome, worker.Aborted, "outcome")
			tk.Equal(t, time.Since(start), 300*time.Millisecond, "elapsed")
			tk.Equal(t, l.Processes()[0].State, tk.Killed, "state")
		})
	})
}
