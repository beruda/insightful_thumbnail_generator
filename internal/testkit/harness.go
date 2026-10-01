package testkit

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/vukkremic/thumbnailer/internal/app"
	"github.com/vukkremic/thumbnailer/internal/domain"
	"github.com/vukkremic/thumbnailer/internal/metrics"
	"github.com/vukkremic/thumbnailer/internal/queue"
	"github.com/vukkremic/thumbnailer/internal/scanner"
	"github.com/vukkremic/thumbnailer/internal/storage"
	"github.com/vukkremic/thumbnailer/internal/worker"
)

const Root = "/videos"

func DefaultConfig() app.Config {
	return app.Config{
		Role:         app.RoleAll,
		Once:         true,
		ScanInterval: time.Minute,
		Scanner: scanner.Config{
			Root:        Root,
			Extensions:  scanner.DefaultVideoExtensions,
			Concurrency: 4,
			MaxAttempts: 3,
			RetryDelay:  100 * time.Millisecond,
		},
		Worker: worker.Config{Concurrency: 4, ShutdownGrace: 5 * time.Second},
		Runner: worker.RunnerConfig{
			ProcessTimeout: time.Minute,
			KillGrace:      time.Second,
			MaxAttempts:    3,
			Backoff:        worker.Backoff{Base: time.Second, Max: 10 * time.Second},
			MaxInputBytes:  1_000_000_000,
			MaxOutputBytes: 1_000_000,
		},
	}
}

// Harness is one fully wired system made of fakes. Unit tests take the piece they need (Runner,
// Scanner, ...); integration tests run the whole Service exactly as main wires it.
// Create it inside a synctest bubble when the test depends on time.
type Harness struct {
	FS       *FS
	Scratch  *Scratch
	Launcher *Launcher
	Blobs    *Blobs
	Repo     *Repo
	Events   *Events
	Queue    *queue.Memory[domain.VideoJob] // video jobs
	Dirs     *queue.Memory[scanner.DirJob]  // directory pages to list
	Metrics  *metrics.Counters
	Logs     *LogRecorder
	Random   func() float64
	Config   app.Config
}

type Option func(*harnessOptions)

type harnessOptions struct {
	queueCapacity int
	seed          uint64
	configure     []func(*app.Config)
}

func WithQueueCapacity(n int) Option { return func(o *harnessOptions) { o.queueCapacity = n } }
func WithSeed(seed uint64) Option    { return func(o *harnessOptions) { o.seed = seed } }
func Configure(f func(*app.Config)) Option {
	return func(o *harnessOptions) { o.configure = append(o.configure, f) }
}

func NewHarness(opts ...Option) *Harness {
	o := harnessOptions{queueCapacity: 1000, seed: 42}
	for _, opt := range opts {
		opt(&o)
	}
	cfg := DefaultConfig()
	for _, f := range o.configure {
		f(&cfg)
	}
	scratch := NewScratch()
	return &Harness{
		FS:       NewFS().Dir(Root),
		Scratch:  scratch,
		Launcher: NewLauncher(scratch),
		Blobs:    &Blobs{MemoryBlobStore: storage.NewMemoryBlobStore()},
		Repo:     &Repo{MemoryRepository: storage.NewMemoryRepository()},
		Events:   &Events{},
		Queue:    queue.NewMemory[domain.VideoJob](o.queueCapacity),
		Dirs:     queue.NewUnbounded[scanner.DirJob](),
		Metrics:  metrics.New(),
		Logs:     &LogRecorder{},
		Random:   Seeded(o.seed),
		Config:   cfg,
	}
}

func (h *Harness) Deps() app.Deps {
	return h.depsWith(h.Queue, h.Dirs)
}

func (h *Harness) depsWith(q queue.Queue[domain.VideoJob], dirs queue.Queue[scanner.DirJob]) app.Deps {
	return app.Deps{
		FS: h.FS, Videos: q, Dirs: dirs, Launch: h.Launcher.Launch, Scratch: h.Scratch, Blobs: h.Blobs,
		Repository: h.Repo, Events: h.Events, Random: h.Random,
		Logger: slog.New(h.Logs), Metrics: h.Metrics,
	}
}

func (h *Harness) Service() *app.Service { return app.NewService(h.Deps(), h.Config) }

func (h *Harness) Scanner() *scanner.Scanner { return h.scannerWith(h.Queue, h.Dirs) }

func (h *Harness) scannerWith(q queue.Queue[domain.VideoJob], dirs queue.Queue[scanner.DirJob]) *scanner.Scanner {
	d := h.depsWith(q, dirs)
	return scanner.New(scanner.Deps{FS: d.FS, Dirs: d.Dirs, Videos: d.Videos, Repository: d.Repository,
		Logger: d.Logger, Metrics: d.Metrics}, h.Config.Scanner)
}

func (h *Harness) Runner() *worker.Runner {
	d := h.Deps()
	return worker.NewRunner(worker.RunnerDeps{FS: d.FS, Launch: d.Launch, Scratch: d.Scratch, Blobs: d.Blobs,
		Repository: d.Repository, Events: d.Events, Random: d.Random, Logger: d.Logger, Metrics: d.Metrics}, h.Config.Runner)
}

// Restarted simulates a process restart: the in-memory queue is gone, but persistent state
// (file system, repository, blobs) survives.
type Restarted struct {
	Queue   *queue.Memory[domain.VideoJob]
	Service *app.Service
	Scanner *scanner.Scanner
}

func (h *Harness) Restart() Restarted {
	q, dirs := queue.NewMemory[domain.VideoJob](1000), queue.NewUnbounded[scanner.DirJob]()
	return Restarted{Queue: q, Service: app.NewService(h.depsWith(q, dirs), h.Config), Scanner: h.scannerWith(q, dirs)}
}

// PendingPaths returns the paths of jobs waiting in q.
func PendingPaths(q *queue.Memory[domain.VideoJob]) []string {
	var out []string
	for _, j := range q.Pending() {
		out = append(out, j.Path)
	}
	return out
}

// Settlement records how a RecordingDelivery was settled.
type Settlement struct {
	Type   string // "ack", "nack", "dead_letter"
	Delay  time.Duration
	Reason string
}

// RecordingDelivery is a queue.Delivery that records its settlement, for runner unit tests.
type RecordingDelivery struct {
	Job     domain.VideoJob
	Count   int
	Settled *Settlement
}

func (d *RecordingDelivery) Message() domain.VideoJob { return d.Job }
func (d *RecordingDelivery) DeliveryCount() int       { return d.Count }
func (d *RecordingDelivery) Ack() error               { return d.settle(Settlement{Type: "ack"}) }
func (d *RecordingDelivery) Nack(delay time.Duration) error {
	return d.settle(Settlement{Type: "nack", Delay: delay})
}
func (d *RecordingDelivery) DeadLetter(reason string) error {
	return d.settle(Settlement{Type: "dead_letter", Reason: reason})
}

func (d *RecordingDelivery) settle(s Settlement) error {
	if d.Settled != nil {
		panic(fmt.Sprintf("delivery settled twice: %s then %s", d.Settled.Type, s.Type))
	}
	d.Settled = &s
	return nil
}

// LogRecorder is a slog.Handler that keeps records for assertions.
type LogRecorder struct {
	mu      sync.Mutex
	entries []LogEntry
}

type LogEntry struct {
	Level   slog.Level
	Message string
	Attrs   map[string]any
}

func (r *LogRecorder) Enabled(context.Context, slog.Level) bool { return true }
func (r *LogRecorder) WithAttrs([]slog.Attr) slog.Handler       { return r }
func (r *LogRecorder) WithGroup(string) slog.Handler            { return r }

func (r *LogRecorder) Handle(_ context.Context, rec slog.Record) error {
	attrs := map[string]any{}
	rec.Attrs(func(a slog.Attr) bool {
		attrs[a.Key] = a.Value.Any()
		return true
	})
	r.mu.Lock()
	defer r.mu.Unlock()
	r.entries = append(r.entries, LogEntry{Level: rec.Level, Message: rec.Message, Attrs: attrs})
	return nil
}

func (r *LogRecorder) Entries() []LogEntry {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]LogEntry(nil), r.entries...)
}

// Has reports whether any entry at level has a message containing substr.
func (r *LogRecorder) Has(level slog.Level, substr string) bool {
	for _, e := range r.Entries() {
		if e.Level == level && strings.Contains(e.Message, substr) {
			return true
		}
	}
	return false
}
