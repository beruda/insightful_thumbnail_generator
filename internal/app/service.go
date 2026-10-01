// Package app wires the scanner and worker together and runs them.
package app

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/vukkremic/thumbnailer/internal/domain"
	"github.com/vukkremic/thumbnailer/internal/metrics"
	"github.com/vukkremic/thumbnailer/internal/queue"
	"github.com/vukkremic/thumbnailer/internal/scanner"
	"github.com/vukkremic/thumbnailer/internal/worker"
)

type Role string

const (
	RoleAll     Role = "all"
	RoleScanner Role = "scanner"
	RoleWorker  Role = "worker"
)

// Deps is every side effect the service has, as one bundle. Tests pass fakes; main passes real adapters.
type Deps struct {
	FS     domain.FileSystem
	Videos queue.Queue[domain.VideoJob]
	// Dirs is the scanner's work queue of directory pages. It must be unbounded (see scanner.Deps).
	Dirs       queue.Queue[scanner.DirJob]
	Launch     domain.Launcher
	Scratch    domain.ScratchSpace
	Blobs      domain.BlobStore
	Repository domain.Repository
	Events     domain.EventPublisher
	Random     func() float64
	Logger     *slog.Logger
	Metrics    *metrics.Counters
}

type Config struct {
	Role Role
	// Once runs a single scan, drains the queue, and returns (instead of rescanning forever).
	Once         bool
	ScanInterval time.Duration
	Scanner      scanner.Config
	Worker       worker.Config
	Runner       worker.RunnerConfig
}

// Service is the composition root. One binary, deployable as a single process (role=all) or as
// separately scaled scanner and worker deployments that share a broker-backed queue.
type Service struct {
	deps    Deps
	cfg     Config
	scanner *scanner.Scanner
	worker  *worker.Worker
}

func NewService(deps Deps, cfg Config) *Service {
	s := &Service{deps: deps, cfg: cfg}
	if cfg.Role == RoleAll || cfg.Role == RoleScanner {
		s.scanner = scanner.New(scanner.Deps{
			FS: deps.FS, Dirs: deps.Dirs, Videos: deps.Videos, Repository: deps.Repository,
			Logger: deps.Logger, Metrics: deps.Metrics,
		}, cfg.Scanner)
	}
	if cfg.Role == RoleAll || cfg.Role == RoleWorker {
		runner := worker.NewRunner(worker.RunnerDeps{
			FS: deps.FS, Launch: deps.Launch, Scratch: deps.Scratch, Blobs: deps.Blobs,
			Repository: deps.Repository, Events: deps.Events, Random: deps.Random,
			Logger: deps.Logger, Metrics: deps.Metrics,
		}, cfg.Runner)
		s.worker = worker.New(deps.Videos, runner, deps.Logger, cfg.Worker)
	}
	return s
}

// Run blocks until ctx is cancelled or, with Once, until one scan has been fully processed.
// A fatal error in one component stops the other too.
func (s *Service) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	errs := make(chan error, 2)
	running := 0
	if s.scanner != nil {
		running++
		go func() { errs <- s.runScanLoop(ctx) }()
	}
	if s.worker != nil {
		running++
		go func() { errs <- s.worker.Run(ctx) }()
	}

	var first error
	for range running {
		if err := <-errs; err != nil && first == nil {
			first = err
			cancel(err) // stop the other component
		}
	}
	return first
}

func (s *Service) runScanLoop(ctx context.Context) error {
	if s.cfg.Once {
		// In one-shot mode, closing the queue lets workers finish once the backlog is drained.
		defer s.deps.Videos.Close()
	}
	for {
		if err := s.scanner.ScanOnce(ctx); err != nil {
			if errors.Is(err, context.Canceled) || ctx.Err() != nil {
				return nil
			}
			s.deps.Logger.Error("scanner failed", "error", err)
			return err
		}
		if s.cfg.Once {
			return nil
		}
		select {
		case <-time.After(s.cfg.ScanInterval):
		case <-ctx.Done():
			return nil
		}
	}
}
