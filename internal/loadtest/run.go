package loadtest

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vukkremic/thumbnailer/internal/app"
	"github.com/vukkremic/thumbnailer/internal/domain"
	"github.com/vukkremic/thumbnailer/internal/metrics"
	"github.com/vukkremic/thumbnailer/internal/queue"
	"github.com/vukkremic/thumbnailer/internal/scanner"
	"github.com/vukkremic/thumbnailer/internal/worker"
)

type Config struct {
	Tree TreeConfig
	// ListLatency / OpenLatency are added to every ListPage / OpenFile call.
	ListLatency, OpenLatency time.Duration
	// Work is the simulated ffmpeg time for a normal video.
	Work            time.Duration
	Concurrency     int // worker pool size
	ScanConcurrency int // directory pages listed in parallel
	QueueCapacity   int
	ProcessTimeout  time.Duration
	// Passes is how many full runs to do over the same tree. Pass 2+ should find everything
	// up to date: it measures the cost of a no-op rescan.
	Passes int
	Logger *slog.Logger // nil discards logs
}

func DefaultConfig() Config {
	return Config{
		Tree: TreeConfig{
			Seed: 1, Files: 100_000, FanOut: 8, FilesPerDir: 100, PageSize: 1000, VideoRatio: 0.9,
			Mix: Mix{Corrupt: 0.01, Slow: 0.005, Hang: 0.0005, Flaky: 0.01},
		},
		Work:            time.Millisecond,
		Concurrency:     64,
		ScanConcurrency: 16,
		QueueCapacity:   1000,
		ProcessTimeout:  2 * time.Second,
		Passes:          2,
	}
}

type Report struct {
	Tree struct {
		Files  int                  `json:"files"`
		Dirs   int                  `json:"dirs"`
		Videos int                  `json:"videos"`
		ByKind map[BehaviorKind]int `json:"byKind"`
	} `json:"tree"`
	Config struct {
		ListLatency     string `json:"listLatency"`
		OpenLatency     string `json:"openLatency"`
		Work            string `json:"work"`
		Concurrency     int    `json:"concurrency"`
		ScanConcurrency int    `json:"scanConcurrency"`
		QueueCapacity   int    `json:"queueCapacity"`
	} `json:"config"`
	Passes []PassReport `json:"passes"`
	// OK is true when every video ended ready or dead-lettered, and the expected ones did.
	OK       bool     `json:"ok"`
	Problems []string `json:"problems,omitempty"`
}

type PassReport struct {
	Pass            int              `json:"pass"`
	Duration        string           `json:"duration"`
	ScanDuration    string           `json:"scanDuration"`
	FilesPerSecond  float64          `json:"scannedFilesPerSecond"`
	VideosPerSecond float64          `json:"processedVideosPerSecond"`
	Published       int64            `json:"published"`
	Generated       int64            `json:"generated"`
	SkippedUpToDate int64            `json:"skippedUpToDate"`
	DeadLettered    map[string]int   `json:"deadLettered"`
	Retries         int64            `json:"retries"`
	Timeouts        int64            `json:"timeouts"`
	ListCalls       int64            `json:"listCalls"`
	OpenCalls       int64            `json:"openCalls"`
	RepoGets        int64            `json:"repoGets"`
	PeakQueueDepth  int              `json:"peakQueueDepth"`
	PeakProcesses   int64            `json:"peakProcesses"`
	PeakGoroutines  int              `json:"peakGoroutines"`
	PeakHeapMB      float64          `json:"peakHeapMB"`
	Counters        map[string]int64 `json:"counters,omitempty"`
}

// Run executes the configured passes and returns the report.
func Run(ctx context.Context, cfg Config) (Report, error) {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	gen := NewGeneratedFS("/bucket", cfg.Tree)
	fsys := &LatencyFS{Inner: gen, ListLatency: cfg.ListLatency, OpenLatency: cfg.OpenLatency}
	repo := NewCompactRepository()
	scratch := NewMemScratch()

	var rep Report
	rep.Tree.Files, rep.Tree.Videos, rep.Tree.ByKind = gen.Census()
	rep.Tree.Dirs = gen.Dirs()
	rep.Config.ListLatency, rep.Config.OpenLatency, rep.Config.Work = cfg.ListLatency.String(), cfg.OpenLatency.String(), cfg.Work.String()
	rep.Config.Concurrency, rep.Config.ScanConcurrency, rep.Config.QueueCapacity = cfg.Concurrency, cfg.ScanConcurrency, cfg.QueueCapacity

	allDead := map[string]int{}
	for pass := 1; pass <= max(1, cfg.Passes); pass++ {
		pr, dead, err := runPass(ctx, cfg, pass, gen, fsys, repo, scratch, logger)
		if err != nil {
			return rep, err
		}
		for k, v := range dead {
			allDead[k] += v
		}
		rep.Passes = append(rep.Passes, pr)
	}

	// Validate end state against the tree's census.
	ready, notified, failed := repo.Counts()
	want := rep.Tree.ByKind
	check := func(ok bool, format string, args ...any) {
		if !ok {
			rep.Problems = append(rep.Problems, fmt.Sprintf(format, args...))
		}
	}
	check(ready+failed == rep.Tree.Videos, "%d ready + %d failed != %d videos", ready, failed, rep.Tree.Videos)
	check(ready == notified, "%d ready but only %d notified", ready, notified)
	check(allDead["corrupt_input"] == want[Corrupt], "corrupt dead letters %d, want %d", allDead["corrupt_input"], want[Corrupt])
	check(allDead["timeout"] == want[Hang], "timeout dead letters %d, want %d", allDead["timeout"], want[Hang])
	check(ready == want[OK]+want[Slow]+want[Flaky], "ready %d, want %d", ready, want[OK]+want[Slow]+want[Flaky])
	check(scratch.Len() == 0, "%d scratch files left", scratch.Len())
	rep.OK = len(rep.Problems) == 0
	return rep, nil
}

func runPass(ctx context.Context, cfg Config, pass int, gen *GeneratedFS, fsys *LatencyFS, repo *CompactRepository,
	scratch *MemScratch, logger *slog.Logger) (PassReport, map[string]int, error) {
	q := &timedQueue{Memory: queue.NewMemory[domain.VideoJob](cfg.QueueCapacity)}
	counters := metrics.New()
	launcher := &Launcher{FS: gen, Scratch: scratch, Work: cfg.Work}
	listsBefore, opensBefore := fsys.Calls()
	getsBefore := repo.gets.Load()

	// Hung videos: timeout on attempt 1, then twice more with backoff, then dead-lettered.
	svc := app.NewService(app.Deps{
		FS: fsys, Videos: q, Dirs: queue.NewUnbounded[scanner.DirJob](), Launch: launcher.Launch, Scratch: scratch, Blobs: &DiscardBlobs{},
		Repository: repo, Events: &CountingEvents{},
		Random: rand.Float64, Logger: logger, Metrics: counters,
	}, app.Config{
		Role: app.RoleAll,
		Once: true,
		Scanner: scanner.Config{
			Root: gen.Root(), Extensions: scanner.DefaultVideoExtensions,
			Concurrency: cfg.ScanConcurrency, MaxAttempts: 5, RetryDelay: 10 * time.Millisecond,
		},
		Worker: worker.Config{Concurrency: cfg.Concurrency},
		Runner: worker.RunnerConfig{
			ProcessTimeout: cfg.ProcessTimeout, KillGrace: 100 * time.Millisecond, MaxAttempts: 3,
			Backoff:       worker.Backoff{Base: 10 * time.Millisecond, Max: 100 * time.Millisecond},
			MaxInputBytes: 1 << 40, MaxOutputBytes: 5 << 20,
		},
	})

	stopSampling := make(chan struct{})
	var peaks peaks
	var wg sync.WaitGroup
	wg.Go(func() { peaks.sample(q, launcher, stopSampling) })

	start := time.Now()
	q.start = start
	err := svc.Run(ctx)
	elapsed := time.Since(start)
	close(stopSampling)
	wg.Wait()
	if err != nil {
		return PassReport{}, nil, err
	}

	lists, opens := fsys.Calls()
	scanElapsed := time.Duration(q.closedAfter.Load())
	dead := map[string]int{}
	for _, d := range q.DeadLetters() {
		kind, _, _ := strings.Cut(d.Reason, ":")
		dead[kind]++
	}
	generated := counters.Get("worker.generated")
	return PassReport{
		Pass:            pass,
		Duration:        elapsed.Round(time.Millisecond).String(),
		ScanDuration:    scanElapsed.Round(time.Millisecond).String(),
		FilesPerSecond:  round(float64(counters.Get("scanner.files_seen")) / scanElapsed.Seconds()),
		VideosPerSecond: round(float64(generated) / elapsed.Seconds()),
		Published:       counters.Get("scanner.published"),
		Generated:       generated,
		SkippedUpToDate: counters.Get("scanner.skipped.up_to_date"),
		DeadLettered:    dead,
		Retries:         counters.Get("worker.retries"),
		Timeouts:        counters.Get("worker.timeouts"),
		ListCalls:       lists - listsBefore,
		OpenCalls:       opens - opensBefore,
		RepoGets:        repo.gets.Load() - getsBefore,
		PeakQueueDepth:  int(peaks.queue.Load()),
		PeakProcesses:   launcher.PeakRunning(),
		PeakGoroutines:  int(peaks.goroutines.Load()),
		PeakHeapMB:      round(float64(peaks.heap.Load()) / (1 << 20)),
	}, dead, nil
}

// timedQueue records when the scanner closed the queue, i.e. when the scan pass finished.
type timedQueue struct {
	*queue.Memory[domain.VideoJob]
	start       time.Time
	closedAfter atomic.Int64
}

func (q *timedQueue) Close() {
	q.closedAfter.Store(int64(time.Since(q.start)))
	q.Memory.Close()
}

type peaks struct {
	queue, goroutines atomic.Int64
	heap              atomic.Uint64
}

func (p *peaks) sample(q *timedQueue, l *Launcher, stop <-chan struct{}) {
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	var ms runtime.MemStats
	for {
		s := q.Stats()
		p.queue.Store(max(p.queue.Load(), int64(s.Ready+s.Delayed+s.InFlight)))
		p.goroutines.Store(max(p.goroutines.Load(), int64(runtime.NumGoroutine())))
		runtime.ReadMemStats(&ms)
		p.heap.Store(max(p.heap.Load(), ms.HeapInuse))
		select {
		case <-stop:
			return
		case <-tick.C:
		}
	}
}

func round(f float64) float64 { return float64(int64(f*10)) / 10 }
