// Package scanner walks the file system and publishes a job for every video whose thumbnail is
// missing or stale. It is cheap I/O work and runs as a single instance; the expensive work
// happens in the workers.
package scanner

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/vukkremic/thumbnailer/internal/domain"
	"github.com/vukkremic/thumbnailer/internal/metrics"
	"github.com/vukkremic/thumbnailer/internal/queue"
)

type Config struct {
	Root            string
	Extensions      []string
	WalkConcurrency int
	FollowSymlinks  bool
	// CheckpointEvery saves a checkpoint every N files. Lower values mean fewer files re-published
	// after a crash (harmless, since workers are idempotent) but more checkpoint writes.
	CheckpointEvery int
}

type Deps struct {
	FS          domain.FileSystem
	Queue       queue.Queue[domain.VideoJob]
	Repository  domain.Repository
	Checkpoints CheckpointStore
	Logger      *slog.Logger
	Metrics     *metrics.Counters
}

type Scanner struct {
	deps       Deps
	cfg        Config
	extensions map[string]bool
}

func New(deps Deps, cfg Config) *Scanner {
	if cfg.CheckpointEvery < 1 {
		cfg.CheckpointEvery = 1
	}
	return &Scanner{deps: deps, cfg: cfg, extensions: ExtensionSet(cfg.Extensions)}
}

// ScanOnce does one full pass over the tree, resuming from a checkpoint if there is one.
// It returns ctx.Err() if cancelled.
func (s *Scanner) ScanOnce(ctx context.Context) error {
	log, m := s.deps.Logger, s.deps.Metrics
	opts := WalkerOptions{
		Concurrency:    s.cfg.WalkConcurrency,
		FollowSymlinks: s.cfg.FollowSymlinks,
		Logger:         log,
		Metrics:        m,
	}

	var walker *Walker
	if pos := s.loadCheckpoint(ctx); pos != nil {
		walker = ResumeWalker(s.deps.FS, *pos, opts)
	} else {
		walker = NewWalker(s.deps.FS, s.cfg.Root, opts)
	}
	log.Info("scan started", "root", s.cfg.Root)

	sinceCheckpoint := 0
	for file := range walker.Files(ctx) {
		m.Inc("scanner.files_seen")
		if reason := Classify(file, s.extensions); reason != Accepted {
			m.Inc("scanner.skipped." + string(reason))
		} else {
			job := domain.JobFor(file)
			if s.upToDate(ctx, job) {
				m.Inc("scanner.skipped.up_to_date")
			} else {
				if err := s.deps.Queue.Publish(ctx, job); err != nil {
					// Note: no checkpoint is saved here, so the unpublished file is retried on resume.
					return fmt.Errorf("publish %s: %w", job.Path, err)
				}
				m.Inc("scanner.published")
			}
		}

		// Saved only after the file was handled. If we crash before this, the checkpoint still
		// points at an earlier file, so files may be re-published but are never skipped.
		sinceCheckpoint++
		if sinceCheckpoint >= s.cfg.CheckpointEvery {
			sinceCheckpoint = 0
			s.saveCheckpoint(ctx, walker)
		}
	}
	if err := walker.Err(); err != nil {
		return err
	}

	if err := s.deps.Checkpoints.Clear(ctx); err != nil {
		return fmt.Errorf("clear checkpoint: %w", err)
	}
	m.Inc("scanner.passes_completed")
	log.Info("scan completed", "root", s.cfg.Root)
	return nil
}

// upToDate fails open: if the repository is down we publish anyway, and the worker checks again.
func (s *Scanner) upToDate(ctx context.Context, job domain.VideoJob) bool {
	record, found, err := s.deps.Repository.Get(ctx, job.Path)
	if err != nil {
		s.deps.Metrics.Inc("scanner.repository_errors")
		s.deps.Logger.Warn("freshness check failed, publishing anyway", "path", job.Path, "error", err)
		return false
	}
	return !domain.NeedsWork(record, found, job.SourceVersion)
}

func (s *Scanner) loadCheckpoint(ctx context.Context) *Position {
	cp, err := s.deps.Checkpoints.Load(ctx)
	if err != nil {
		s.deps.Logger.Warn("cannot load checkpoint, starting from scratch", "error", err)
		return nil
	}
	if cp == nil || cp.Root != s.cfg.Root {
		return nil
	}
	s.deps.Logger.Info("resuming scan from checkpoint")
	return &cp.Position
}

func (s *Scanner) saveCheckpoint(ctx context.Context, w *Walker) {
	err := s.deps.Checkpoints.Save(ctx, Checkpoint{Root: s.cfg.Root, Position: w.Position()})
	if err != nil {
		// A lost checkpoint only costs re-work after a crash; not worth failing the scan over.
		s.deps.Metrics.Inc("scanner.checkpoint_errors")
		s.deps.Logger.Warn("cannot save checkpoint", "error", err)
	}
}
