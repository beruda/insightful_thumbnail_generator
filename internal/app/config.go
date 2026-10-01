package app

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"runtime"
	"time"

	"github.com/vukkremic/thumbnailer/internal/scanner"
	"github.com/vukkremic/thumbnailer/internal/worker"
)

// Options is everything main needs: the service config plus process-level settings.
type Options struct {
	Service       Config
	QueueCapacity int
	DataDir       string
	FFmpegPath    string
	ThumbWidth    int
	HealthAddr    string
	LogLevel      slog.Level
}

type DataPaths struct {
	Blobs, Database, Checkpoint, Scratch string
}

func (o Options) Paths() DataPaths {
	return DataPaths{
		Blobs:      filepath.Join(o.DataDir, "blobs"),
		Database:   filepath.Join(o.DataDir, "thumbnails.sqlite"),
		Checkpoint: filepath.Join(o.DataDir, "scan-checkpoint.json"),
		Scratch:    filepath.Join(o.DataDir, "scratch"),
	}
}

// ParseFlags builds Options from command-line arguments. stderr receives usage output.
func ParseFlags(args []string, stderr io.Writer) (Options, error) {
	fs := flag.NewFlagSet("thumbnailer", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var (
		root           = fs.String("root", "", "directory to scan (required unless -role=worker)")
		role           = fs.String("role", "all", "all | scanner | worker")
		once           = fs.Bool("once", false, "scan once, drain the queue, print a summary and exit")
		dataDir        = fs.String("data-dir", "./data", "thumbnails, metadata DB, checkpoint and scratch files")
		concurrency    = fs.Int("concurrency", runtime.NumCPU(), "parallel ffmpeg processes")
		timeout        = fs.Duration("timeout", 2*time.Minute, "per-process time limit")
		maxAttempts    = fs.Int("max-attempts", 4, "attempts before dead-lettering")
		maxInputMB     = fs.Int64("max-input-mb", 20*1024, "skip larger videos")
		scanInterval   = fs.Duration("scan-interval", 5*time.Minute, "delay between scans without -once")
		followSymlinks = fs.Bool("follow-symlinks", false, "follow symlinks (loops are detected)")
		healthAddr     = fs.String("health-addr", "", "serve GET /healthz on this address, e.g. :8080")
		ffmpegPath     = fs.String("ffmpeg", "ffmpeg", "ffmpeg binary")
		logLevel       = fs.String("log-level", "info", "debug | info | warn | error")
	)
	if err := fs.Parse(args); err != nil {
		return Options{}, err
	}

	r := Role(*role)
	switch {
	case r != RoleAll && r != RoleScanner && r != RoleWorker:
		return Options{}, fmt.Errorf("-role must be all, scanner or worker, got %q", *role)
	case r != RoleWorker && *root == "":
		return Options{}, errors.New("-root is required")
	case *concurrency < 1, *maxAttempts < 1, *maxInputMB < 1, *timeout <= 0, *scanInterval <= 0:
		return Options{}, errors.New("-concurrency, -max-attempts, -max-input-mb, -timeout and -scan-interval must be positive")
	}
	var level slog.Level
	if err := level.UnmarshalText([]byte(*logLevel)); err != nil {
		return Options{}, fmt.Errorf("-log-level: %w", err)
	}
	absRoot, err := filepath.Abs(*root)
	if err != nil {
		return Options{}, err
	}
	absData, err := filepath.Abs(*dataDir)
	if err != nil {
		return Options{}, err
	}

	return Options{
		Service: Config{
			Role:         r,
			Once:         *once,
			ScanInterval: *scanInterval,
			Scanner: scanner.Config{
				Root:            absRoot,
				Extensions:      scanner.DefaultVideoExtensions,
				WalkConcurrency: 16,
				FollowSymlinks:  *followSymlinks,
				CheckpointEvery: 100,
			},
			Worker: worker.Config{Concurrency: *concurrency, ShutdownGrace: 10 * time.Second},
			Runner: worker.RunnerConfig{
				ProcessTimeout: *timeout,
				KillGrace:      5 * time.Second,
				MaxAttempts:    *maxAttempts,
				Backoff:        worker.Backoff{Base: 5 * time.Second, Max: 5 * time.Minute},
				MaxInputBytes:  *maxInputMB * 1024 * 1024,
				MaxOutputBytes: 5 * 1024 * 1024,
			},
		},
		QueueCapacity: max(100, *concurrency*10),
		DataDir:       absData,
		FFmpegPath:    *ffmpegPath,
		ThumbWidth:    320,
		HealthAddr:    *healthAddr,
		LogLevel:      level,
	}, nil
}
