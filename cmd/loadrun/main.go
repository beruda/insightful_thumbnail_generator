// Command loadrun runs the real service against a synthetic file system of any size and prints a
// JSON report: throughput, call counts, and resource peaks. Nothing touches the disk.
//
//	go run ./cmd/loadrun -files 1000000 -open-latency 2ms
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"

	"github.com/vukkremic/thumbnailer/internal/loadtest"
)

func main() {
	cfg := loadtest.DefaultConfig()
	flag.IntVar(&cfg.Tree.Files, "files", cfg.Tree.Files, "total files in the generated tree")
	flag.IntVar(&cfg.Tree.FanOut, "fanout", cfg.Tree.FanOut, "subdirectories per directory")
	flag.IntVar(&cfg.Tree.FilesPerDir, "files-per-dir", cfg.Tree.FilesPerDir, "files per directory")
	flag.Uint64Var(&cfg.Tree.Seed, "seed", cfg.Tree.Seed, "tree seed")
	flag.Float64Var(&cfg.Tree.VideoRatio, "video-ratio", cfg.Tree.VideoRatio, "fraction of files that are videos")
	flag.Float64Var(&cfg.Tree.Mix.Corrupt, "corrupt", cfg.Tree.Mix.Corrupt, "fraction of videos that are corrupt")
	flag.Float64Var(&cfg.Tree.Mix.Slow, "slow", cfg.Tree.Mix.Slow, "fraction of videos that take 50× longer")
	flag.Float64Var(&cfg.Tree.Mix.Hang, "hang", cfg.Tree.Mix.Hang, "fraction of videos that hang until killed")
	flag.Float64Var(&cfg.Tree.Mix.Flaky, "flaky", cfg.Tree.Mix.Flaky, "fraction of videos that fail twice, then succeed")
	flag.DurationVar(&cfg.ListLatency, "list-latency", cfg.ListLatency, "added latency per ListFiles call")
	flag.DurationVar(&cfg.OpenLatency, "open-latency", cfg.OpenLatency, "added latency per OpenFile call")
	flag.DurationVar(&cfg.Work, "work", cfg.Work, "simulated ffmpeg time per normal video")
	flag.IntVar(&cfg.Concurrency, "concurrency", cfg.Concurrency, "worker pool size")
	flag.IntVar(&cfg.WalkConcurrency, "walk-concurrency", cfg.WalkConcurrency, "parallel OpenFile calls while walking")
	flag.IntVar(&cfg.QueueCapacity, "queue-capacity", cfg.QueueCapacity, "job queue capacity")
	flag.DurationVar(&cfg.ProcessTimeout, "timeout", cfg.ProcessTimeout, "per-process timeout (hung videos hit it)")
	flag.IntVar(&cfg.Passes, "passes", cfg.Passes, "full runs over the same tree (2nd+ should skip everything)")
	verbose := flag.Bool("v", false, "log warnings and errors to stderr")
	flag.Parse()

	if *verbose {
		cfg.Logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	report, err := loadtest.Run(ctx, cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, "loadrun:", err)
		os.Exit(1)
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	_ = enc.Encode(report)
	if !report.OK {
		os.Exit(1)
	}
}
