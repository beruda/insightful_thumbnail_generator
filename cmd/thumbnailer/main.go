// Command thumbnailer scans a directory tree for videos and generates thumbnails with ffmpeg.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/vukkremic/thumbnailer/internal/adapters"
	"github.com/vukkremic/thumbnailer/internal/app"
	"github.com/vukkremic/thumbnailer/internal/domain"
	"github.com/vukkremic/thumbnailer/internal/metrics"
	"github.com/vukkremic/thumbnailer/internal/queue"
	"github.com/vukkremic/thumbnailer/internal/scanner"
)

func main() {
	os.Exit(run())
}

func run() int {
	opts, err := app.ParseFlags(os.Args[1:], os.Stderr)
	if errors.Is(err, flag.ErrHelp) {
		return 0
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	if opts.Service.Role != app.RoleAll {
		// Separate scanner/worker deployments need a shared broker. Only the in-memory queue exists
		// so far; a Pub/Sub or SQS adapter implementing queue.Queue is the next step for that topology.
		fmt.Fprintf(os.Stderr, "-role=%s needs a broker-backed queue adapter (not implemented yet); use -role=all\n", opts.Service.Role)
		return 2
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: opts.LogLevel})).With("role", opts.Service.Role)
	if err := serve(opts, logger); err != nil {
		logger.Error("fatal", "error", err)
		return 1
	}
	return 0
}

func serve(opts app.Options, logger *slog.Logger) error {
	paths := opts.Paths()
	if err := os.MkdirAll(opts.DataDir, 0o755); err != nil {
		return err
	}
	repo, err := adapters.OpenSQLite(paths.Database)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer repo.Close()

	counters := metrics.New()
	jobs := queue.NewMemory[domain.VideoJob](opts.QueueCapacity)
	service := app.NewService(app.Deps{
		FS:         adapters.OSFileSystem{},
		Videos:     jobs,
		Dirs:       queue.NewUnbounded[scanner.DirJob](),
		Launch:     adapters.FFmpegLauncher(opts.FFmpegPath, opts.ThumbWidth),
		Scratch:    adapters.ScratchDir{Dir: paths.Scratch},
		Blobs:      adapters.LocalBlobStore{Root: paths.Blobs},
		Repository: repo,
		Events:     app.LogEvents{Logger: logger},
		Random:     rand.Float64,
		Logger:     logger,
		Metrics:    counters,
	}, opts.Service)

	if opts.HealthAddr != "" {
		server := &http.Server{Addr: opts.HealthAddr, Handler: app.HealthHandler(func() map[string]any {
			return map[string]any{"queue": jobs.Stats(), "metrics": counters.Snapshot()}
		})}
		go func() {
			if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				logger.Error("health server failed", "error", err)
			}
		}()
		defer server.Close()
	}

	// First SIGINT/SIGTERM: graceful shutdown. Second one: exit immediately (NotifyContext stops
	// catching signals once ctx is done, so the default handler kills the process).
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger.Info("service starting", "root", opts.Service.Scanner.Root, "dataDir", opts.DataDir,
		"concurrency", opts.Service.Worker.Concurrency, "once", opts.Service.Once)
	runErr := service.Run(ctx)
	stop()

	logger.Info("service stopped", "queue", jobs.Stats(), "metrics", counters.Snapshot(), "thumbnails", paths.Blobs)
	for _, dead := range jobs.DeadLetters() {
		logger.Warn("dead letter", "path", dead.Message.Path, "reason", dead.Reason, "attempts", dead.DeliveryCount)
	}
	return runErr
}
