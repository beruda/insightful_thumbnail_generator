package worker

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/vukkremic/thumbnailer/internal/domain"
	"github.com/vukkremic/thumbnailer/internal/queue"
)

type Config struct {
	// Concurrency caps concurrent ffmpeg processes on this instance (roughly the CPU count).
	Concurrency int
	// ShutdownGrace is how long in-flight jobs get to finish after shutdown starts, before their
	// processes are killed.
	ShutdownGrace time.Duration
}

// Worker consumes jobs with bounded concurrency. Shutdown happens in two phases:
//  1. ctx is cancelled → stop taking new jobs, let in-flight ones finish;
//  2. after ShutdownGrace → kill whatever ffmpeg is still running and return those jobs to the queue.
type Worker struct {
	queue  queue.Queue[domain.VideoJob]
	runner *Runner
	logger *slog.Logger
	cfg    Config
}

func New(q queue.Queue[domain.VideoJob], runner *Runner, logger *slog.Logger, cfg Config) *Worker {
	return &Worker{queue: q, runner: runner, logger: logger, cfg: cfg}
}

func (w *Worker) Run(ctx context.Context) error {
	// hardStop outlives ctx by ShutdownGrace; it's what actually kills running processes.
	hardStop, killNow := context.WithCancel(context.WithoutCancel(ctx))
	defer killNow()
	stopWatcher := context.AfterFunc(ctx, func() {
		w.logger.Info("worker draining", "grace", w.cfg.ShutdownGrace)
		if w.cfg.ShutdownGrace <= 0 {
			killNow()
			return
		}
		timer := time.AfterFunc(w.cfg.ShutdownGrace, killNow)
		context.AfterFunc(hardStop, func() { timer.Stop() }) // drained before the grace ran out
	})
	defer stopWatcher()

	return w.queue.Consume(ctx, w.cfg.Concurrency, func(d queue.Delivery[domain.VideoJob]) (err error) {
		defer func() {
			if recovered := recover(); recovered != nil {
				// A bug, not a job failure. The queue redelivers the message, so it isn't lost.
				w.logger.Error("panic handling job", "path", d.Message().Path, "panic", recovered)
				err = fmt.Errorf("panic: %v", recovered)
			}
		}()
		w.runner.Handle(hardStop, d)
		return nil
	})
}
