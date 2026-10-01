// Package worker turns queued jobs into thumbnails: it runs ffmpeg under supervision, validates
// the output, stores it, and signals completion.
package worker

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"time"

	"github.com/vukkremic/thumbnailer/internal/domain"
	"github.com/vukkremic/thumbnailer/internal/metrics"
	"github.com/vukkremic/thumbnailer/internal/queue"
	"github.com/vukkremic/thumbnailer/internal/storage"
)

type RunnerConfig struct {
	ProcessTimeout time.Duration
	KillGrace      time.Duration
	// MaxAttempts is the total number of deliveries before a retryable failure is dead-lettered.
	MaxAttempts    int
	Backoff        Backoff
	MaxInputBytes  int64
	MaxOutputBytes int
}

type RunnerDeps struct {
	FS         domain.FileSystem
	Launch     domain.Launcher
	Scratch    domain.ScratchSpace
	Blobs      domain.BlobStore
	Repository domain.Repository
	Events     domain.EventPublisher
	Random     func() float64 // in [0, 1); seeded in tests so jitter is reproducible
	Now        func() time.Time
	Logger     *slog.Logger
	Metrics    *metrics.Counters
}

type Outcome string

const (
	Generated      Outcome = "generated"
	AlreadySettled Outcome = "already_settled" // thumbnail exists, or failed permanently, for this version
	Superseded     Outcome = "superseded"      // the source changed or disappeared after publishing
	RetryScheduled Outcome = "retry_scheduled"
	DeadLettered   Outcome = "dead_lettered"
	Released       Outcome = "released" // shutdown: returned to the queue untouched
	NotifyDeferred Outcome = "notify_deferred"
)

// Runner handles one delivery: check → generate → validate → store → record → notify → ack.
//
// Delivery is at-least-once, so Handle is idempotent:
//   - a job whose version is already settled is acked without running ffmpeg;
//   - the blob key is deterministic, so a retry after a partial commit overwrites the same object;
//   - the record's Notified flag lets a retry (or the next scan) send an event a previous attempt couldn't.
//
// Retries go through the queue (nack with a backoff delay) rather than sleeping in the worker,
// so a failing job never holds a worker slot and a retry survives a worker crash.
type Runner struct {
	deps RunnerDeps
	cfg  RunnerConfig
}

func NewRunner(deps RunnerDeps, cfg RunnerConfig) *Runner {
	if deps.Now == nil {
		deps.Now = time.Now
	}
	return &Runner{deps: deps, cfg: cfg}
}

// Handle processes one delivery. ctx is the hard-stop context: cancelling it kills a running ffmpeg.
func (r *Runner) Handle(ctx context.Context, d queue.Delivery[domain.VideoJob]) Outcome {
	job := d.Message()

	existing, found, err := r.deps.Repository.Get(ctx, job.Path)
	if err != nil {
		return r.fail(ctx, d, storageFailure("repository read", err))
	}
	if domain.IsSettled(existing, found, job.SourceVersion) {
		return r.finishSettled(ctx, d, existing)
	}

	if job.Size > r.cfg.MaxInputBytes {
		return r.fail(ctx, d, Failure{
			Kind:    KindInputTooLarge,
			Message: fmt.Sprintf("input is %d bytes, limit is %d", job.Size, r.cfg.MaxInputBytes),
		})
	}

	switch state, failure := r.checkSource(ctx, job); state {
	case sourceChanged, sourceGone:
		r.deps.Metrics.Inc("worker.superseded." + string(state))
		_ = d.Ack()
		return Superseded
	case sourceUnknown:
		return r.fail(ctx, d, failure)
	}

	if ctx.Err() != nil {
		return r.release(d)
	}
	data, contentType, failure, aborted := r.generate(ctx, job)
	if aborted {
		return r.release(d)
	}
	if failure != nil {
		return r.fail(ctx, d, *failure)
	}
	return r.commit(ctx, d, data, contentType)
}

type sourceState string

const (
	sourceUnchanged sourceState = "unchanged"
	sourceChanged   sourceState = "changed"
	sourceGone      sourceState = "gone"
	sourceUnknown   sourceState = "unknown"
)

// checkSource re-reads the source before the expensive work: the job may have sat in the queue
// while the file was replaced (a newer job is coming) or deleted (nothing to do).
func (r *Runner) checkSource(ctx context.Context, job domain.VideoJob) (sourceState, Failure) {
	info, err := r.deps.FS.OpenFile(ctx, job.Path)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return sourceGone, Failure{}
	case err != nil:
		return sourceUnknown, Failure{Kind: KindInputUnavailable, Retryable: true, Message: fmt.Sprintf("cannot stat source: %v", err)}
	case domain.SourceVersion(info) != job.SourceVersion:
		return sourceChanged, Failure{}
	}
	return sourceUnchanged, Failure{}
}

func (r *Runner) generate(ctx context.Context, job domain.VideoJob) (data []byte, contentType string, failure *Failure, aborted bool) {
	log, m := r.deps.Logger, r.deps.Metrics
	outputPath, err := r.deps.Scratch.Allocate(".jpg")
	if err != nil {
		f := Failure{Kind: KindSpawnError, Retryable: true, Message: fmt.Sprintf("allocate scratch file: %v", err)}
		return nil, "", &f, false
	}
	defer func() {
		if err := r.deps.Scratch.Remove(outputPath); err != nil {
			log.Warn("cannot remove scratch file", "path", outputPath, "error", err)
		}
	}()

	process, err := r.deps.Launch(job.Path, outputPath)
	if err != nil {
		return nil, "", &Failure{Kind: KindSpawnError, Retryable: true, Message: err.Error()}, false
	}
	m.Inc("worker.processes_started")

	result := RunProcess(ctx, process, r.cfg.ProcessTimeout, r.cfg.KillGrace)
	if !result.Reaped {
		m.Inc("worker.processes_unreaped")
		log.Error("ffmpeg ignored kill; abandoning process", "path", job.Path)
	}
	switch {
	case result.Outcome == Aborted:
		return nil, "", nil, true
	case result.Outcome == TimedOut:
		m.Inc("worker.timeouts")
		return nil, "", &Failure{Kind: KindTimeout, Retryable: true, Message: fmt.Sprintf("ffmpeg exceeded %s", r.cfg.ProcessTimeout)}, false
	case result.ExitCode != 0:
		f := ClassifyProcessFailure(result.ExitCode, result.Stderr)
		return nil, "", &f, false
	}

	output, err := r.deps.Scratch.Read(outputPath)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, "", &Failure{Kind: KindInvalidOutput, Retryable: true, Message: fmt.Sprintf("read output: %v", err)}, false
	}
	contentType, f := r.validateOutput(output, err == nil)
	return output, contentType, f, false
}

// validateOutput doesn't trust a zero exit code: ffmpeg can "succeed" without a usable image.
func (r *Runner) validateOutput(data []byte, exists bool) (string, *Failure) {
	invalid := func(msg string) (string, *Failure) {
		return "", &Failure{Kind: KindInvalidOutput, Retryable: true, Message: msg}
	}
	switch {
	case !exists:
		return invalid("ffmpeg exited 0 but wrote no output")
	case len(data) == 0:
		return invalid("ffmpeg wrote an empty file")
	case len(data) > r.cfg.MaxOutputBytes:
		return "", &Failure{Kind: KindOutputTooLarge, Message: fmt.Sprintf("output is %d bytes, limit is %d", len(data), r.cfg.MaxOutputBytes)}
	}
	contentType, ok := DetectImageType(data)
	if !ok {
		return invalid("output is not a recognizable image")
	}
	return contentType, nil
}

func (r *Runner) commit(ctx context.Context, d queue.Delivery[domain.VideoJob], data []byte, contentType string) Outcome {
	job := d.Message()
	key := storage.BlobKey(job.Path, job.SourceVersion, contentType)
	record := domain.ThumbnailRecord{
		VideoPath:     job.Path,
		SourceVersion: job.SourceVersion,
		Status:        domain.StatusReady,
		BlobKey:       key,
		ByteLength:    len(data),
		ContentType:   contentType,
		Attempts:      d.DeliveryCount(),
		UpdatedAt:     r.deps.Now(),
	}

	// Order matters: blob → record → event. A reader never sees a record pointing at a missing
	// blob, and the event only goes out once both exist.
	if err := r.deps.Blobs.Put(ctx, key, data, contentType); err != nil {
		return r.fail(ctx, d, storageFailure("blob put", err))
	}
	if err := r.deps.Repository.Upsert(ctx, record); err != nil {
		return r.fail(ctx, d, storageFailure("record upsert", err))
	}
	r.deps.Metrics.Inc("worker.generated")
	r.deps.Logger.Info("thumbnail generated", "path", job.Path, "blobKey", key, "attempt", d.DeliveryCount())

	// From here on the thumbnail exists: nothing below may turn it into a failure.
	if err := r.notify(ctx, record); err != nil {
		return r.notifyFailed(d, err)
	}
	_ = d.Ack()
	return Generated
}

func (r *Runner) finishSettled(ctx context.Context, d queue.Delivery[domain.VideoJob], record domain.ThumbnailRecord) Outcome {
	if record.Status == domain.StatusReady && !record.Notified {
		if err := r.notify(ctx, record); err != nil {
			return r.notifyFailed(d, err)
		}
	}
	_ = d.Ack()
	r.deps.Metrics.Inc("worker.already_settled")
	return AlreadySettled
}

func (r *Runner) notify(ctx context.Context, record domain.ThumbnailRecord) error {
	err := r.deps.Events.Publish(ctx, domain.ReadyEvent{
		Type:          "thumbnail.ready",
		VideoPath:     record.VideoPath,
		SourceVersion: record.SourceVersion,
		BlobKey:       record.BlobKey,
		ContentType:   record.ContentType,
	})
	if err != nil {
		return fmt.Errorf("publish event: %w", err)
	}
	record.Notified = true
	record.UpdatedAt = r.deps.Now()
	if err := r.deps.Repository.Upsert(ctx, record); err != nil {
		return fmt.Errorf("mark notified: %w", err)
	}
	return nil
}

// notifyFailed handles a failure after the thumbnail was committed. The video is done, so it must
// never be dead-lettered or marked failed. Retry while attempts remain; after that, ack and leave
// Notified=false, and the next scan re-publishes the job to send the event (see domain.NeedsWork).
func (r *Runner) notifyFailed(d queue.Delivery[domain.VideoJob], err error) Outcome {
	m, log := r.deps.Metrics, r.deps.Logger
	m.Inc("worker.notify_errors")
	attempt := d.DeliveryCount()
	if attempt < r.cfg.MaxAttempts {
		delay := r.cfg.Backoff.Delay(attempt, r.deps.Random)
		log.Warn("ready event not sent, will retry", "path", d.Message().Path, "attempt", attempt, "delay", delay, "error", err)
		_ = d.Nack(delay)
		m.Inc("worker.retries")
		return RetryScheduled
	}
	log.Error("ready event not sent; deferring to the next scan", "path", d.Message().Path, "error", err)
	_ = d.Ack()
	m.Inc("worker.notify_deferred")
	return NotifyDeferred
}

func (r *Runner) fail(ctx context.Context, d queue.Delivery[domain.VideoJob], f Failure) Outcome {
	m, log := r.deps.Metrics, r.deps.Logger
	job, attempt := d.Message(), d.DeliveryCount()
	m.Inc("worker.failures." + string(f.Kind))

	if f.Retryable && attempt < r.cfg.MaxAttempts {
		delay := r.cfg.Backoff.Delay(attempt, r.deps.Random)
		log.Warn("thumbnail attempt failed, will retry", "path", job.Path, "attempt", attempt, "delay", delay, "kind", f.Kind, "error", f.Message)
		_ = d.Nack(delay)
		m.Inc("worker.retries")
		return RetryScheduled
	}

	// A duplicate delivery may have succeeded concurrently; never downgrade a ready thumbnail.
	// (Fully closing this race needs a conditional write in the database: "unless status = ready".)
	if rec, found, err := r.deps.Repository.Get(ctx, job.Path); err == nil && found &&
		rec.SourceVersion == job.SourceVersion && rec.Status == domain.StatusReady {
		_ = d.Ack()
		m.Inc("worker.already_settled")
		return AlreadySettled
	}

	log.Error("thumbnail failed permanently", "path", job.Path, "attempt", attempt, "kind", f.Kind, "error", f.Message)
	// Recording the failure stops the scanner from re-publishing this version on every pass.
	err := r.deps.Repository.Upsert(ctx, domain.ThumbnailRecord{
		VideoPath:     job.Path,
		SourceVersion: job.SourceVersion,
		Status:        domain.StatusFailed,
		FailureKind:   string(f.Kind),
		FailureReason: f.Message,
		Attempts:      attempt,
		UpdatedAt:     r.deps.Now(),
	})
	if err != nil {
		log.Warn("cannot record failure", "path", job.Path, "error", err)
	}
	_ = d.DeadLetter(f.Error())
	m.Inc("worker.dead_lettered")
	return DeadLettered
}

func (r *Runner) release(d queue.Delivery[domain.VideoJob]) Outcome {
	_ = d.Nack(0)
	r.deps.Metrics.Inc("worker.released")
	return Released
}
