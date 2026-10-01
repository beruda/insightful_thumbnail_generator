// Package scanner finds videos whose thumbnail is missing or stale and publishes a job for each.
//
// The walk itself runs on a work queue: each directory page is a DirJob, and listing a page
// publishes a DirJob for each subdirectory (and for the next page). Any number of scanner
// goroutines, or replicas sharing a broker-backed queue, can list in parallel. The queue doubles
// as the checkpoint: an unfinished page is simply redelivered.
package scanner

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/vukkremic/thumbnailer/internal/domain"
	"github.com/vukkremic/thumbnailer/internal/metrics"
	"github.com/vukkremic/thumbnailer/internal/queue"
)

// DirJob is one page of one directory to list.
type DirJob struct {
	Dir       string `json:"dir"`
	PageToken string `json:"pageToken,omitempty"`
}

type Config struct {
	Root       string
	Extensions []string
	// Concurrency is how many directory pages are listed in parallel.
	Concurrency int
	// MaxAttempts is how many times a failing page is listed before it's dead-lettered.
	MaxAttempts int
	// RetryDelay is the wait before relisting a failed page (multiplied by the attempt number).
	RetryDelay time.Duration
}

type Deps struct {
	FS domain.FileSystem
	// Dirs must be unbounded: scanner goroutines both consume it and publish to it, so a full
	// queue would block every consumer on a publish that only they could make room for.
	// Backpressure comes from Videos instead.
	Dirs       queue.Queue[DirJob]
	Videos     queue.Queue[domain.VideoJob]
	Repository domain.Repository
	Logger     *slog.Logger
	Metrics    *metrics.Counters
}

type Scanner struct {
	deps       Deps
	cfg        Config
	extensions map[string]bool
}

func New(deps Deps, cfg Config) *Scanner {
	if cfg.MaxAttempts < 1 {
		cfg.MaxAttempts = 1
	}
	return &Scanner{deps: deps, cfg: cfg, extensions: ExtensionSet(cfg.Extensions)}
}

// ScanOnce does one full pass over the tree and returns when every directory page has been
// listed (or dead-lettered). It returns ctx.Err() if cancelled.
//
// Completion is tracked with an outstanding-job counter: +1 before each DirJob is published, −1
// once a job is acked or dead-lettered. When it reaches zero, nothing is queued or in flight, so
// the pass is done. (Several scanner replicas would keep this counter in shared storage.)
func (s *Scanner) ScanOnce(ctx context.Context) error {
	passCtx, passDone := context.WithCancel(ctx)
	defer passDone()

	var outstanding atomic.Int64
	outstanding.Add(1)
	if err := s.deps.Dirs.Publish(passCtx, DirJob{Dir: s.cfg.Root}); err != nil {
		return fmt.Errorf("publish root: %w", err)
	}
	s.deps.Logger.Info("scan started", "root", s.cfg.Root)

	err := s.deps.Dirs.Consume(passCtx, s.cfg.Concurrency, func(d queue.Delivery[DirJob]) error {
		if s.handle(passCtx, d, &outstanding) && outstanding.Add(-1) == 0 {
			passDone()
		}
		return nil
	})
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s.deps.Metrics.Inc("scanner.passes_completed")
	s.deps.Logger.Info("scan completed", "root", s.cfg.Root)
	return nil
}

// handle lists one page and publishes its subdirectories, its next page, and the videos that
// need work. It returns true once the job is finished (acked or dead-lettered), false if it was
// returned to the queue.
func (s *Scanner) handle(ctx context.Context, d queue.Delivery[DirJob], outstanding *atomic.Int64) bool {
	job, m := d.Message(), s.deps.Metrics
	page, err := s.deps.FS.ListPage(ctx, job.Dir, job.PageToken)
	if err != nil {
		return s.listFailed(ctx, d, err)
	}
	m.Inc("scanner.pages_listed")

	// Subdirectories and the next page go first, so other scanners can start on them while this
	// one may block publishing videos (that's where backpressure applies).
	publishDir := func(next DirJob) bool {
		outstanding.Add(1)
		if err := s.deps.Dirs.Publish(ctx, next); err != nil {
			outstanding.Add(-1)
			return false
		}
		return true
	}
	if page.NextToken != "" && !publishDir(DirJob{Dir: job.Dir, PageToken: page.NextToken}) {
		return s.release(d)
	}

	var candidates []domain.VideoJob
	for _, e := range page.Entries {
		switch {
		case e.IsSymlink:
			m.Inc("scanner.skipped.symlink")
		case e.IsDir:
			if !publishDir(DirJob{Dir: e.Path}) {
				return s.release(d)
			}
		default:
			m.Inc("scanner.files_seen")
			if reason := Classify(e, s.extensions); reason != Accepted {
				m.Inc("scanner.skipped." + string(reason))
			} else {
				candidates = append(candidates, domain.JobFor(e))
			}
		}
	}

	for _, job := range s.needingWork(ctx, candidates) {
		if err := s.deps.Videos.Publish(ctx, job); err != nil {
			// Shutdown. The page is relisted on redelivery; re-published videos are harmless
			// because workers are idempotent.
			return s.release(d)
		}
		m.Inc("scanner.published")
	}
	_ = d.Ack()
	return true
}

// needingWork filters candidates with one batched repository lookup. It fails open: if the
// repository is down, everything is published and the worker checks again.
func (s *Scanner) needingWork(ctx context.Context, candidates []domain.VideoJob) []domain.VideoJob {
	if len(candidates) == 0 {
		return nil
	}
	paths := make([]string, len(candidates))
	for i, c := range candidates {
		paths[i] = c.Path
	}
	records, err := s.deps.Repository.GetMany(ctx, paths)
	if err != nil {
		s.deps.Metrics.Inc("scanner.repository_errors")
		s.deps.Logger.Warn("freshness check failed, publishing anyway", "count", len(paths), "error", err)
		return candidates
	}
	out := candidates[:0]
	for _, c := range candidates {
		rec, found := records[c.Path]
		if domain.NeedsWork(rec, found, c.SourceVersion) {
			out = append(out, c)
		} else {
			s.deps.Metrics.Inc("scanner.skipped.up_to_date")
		}
	}
	return out
}

func (s *Scanner) listFailed(ctx context.Context, d queue.Delivery[DirJob], err error) bool {
	job, m, log := d.Message(), s.deps.Metrics, s.deps.Logger
	switch {
	case ctx.Err() != nil:
		return s.release(d)
	case errors.Is(err, fs.ErrNotExist):
		// Deleted between being listed by its parent and being listed itself: normal churn.
		m.Inc("scanner.vanished")
		_ = d.Ack()
		return true
	case d.DeliveryCount() < s.cfg.MaxAttempts:
		delay := s.cfg.RetryDelay * time.Duration(d.DeliveryCount())
		m.Inc("scanner.list_retries")
		log.Warn("cannot list directory, will retry", "dir", job.Dir, "attempt", d.DeliveryCount(), "delay", delay, "error", err)
		_ = d.Nack(delay)
		return false
	default:
		m.Inc("scanner.list_errors")
		log.Error("cannot list directory, skipping it", "dir", job.Dir, "attempts", d.DeliveryCount(), "error", err)
		_ = d.DeadLetter(err.Error())
		return true
	}
}

func (s *Scanner) release(d queue.Delivery[DirJob]) bool {
	_ = d.Nack(0)
	return false
}
