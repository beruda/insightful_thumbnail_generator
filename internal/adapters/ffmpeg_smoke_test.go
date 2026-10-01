package adapters_test

import (
	"context"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/vukkremic/thumbnailer/internal/adapters"
	"github.com/vukkremic/thumbnailer/internal/app"
	"github.com/vukkremic/thumbnailer/internal/domain"
	"github.com/vukkremic/thumbnailer/internal/metrics"
	"github.com/vukkremic/thumbnailer/internal/queue"
	"github.com/vukkremic/thumbnailer/internal/scanner"
	tk "github.com/vukkremic/thumbnailer/internal/testkit"
	"github.com/vukkremic/thumbnailer/internal/worker"
)

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// Smoke tests run the real ffmpeg (skipped if it isn't installed, or with -short) to check that
// the launcher, the kill path and the stderr classifier match how ffmpeg actually behaves.
func requireFFmpeg(t *testing.T) string {
	t.Helper()
	if testing.Short() {
		t.Skip("smoke test; skipped with -short")
	}
	path, err := exec.LookPath("ffmpeg")
	if err != nil {
		t.Skip("ffmpeg not installed")
	}
	return path
}

func ffmpeg(t *testing.T, bin string, args ...string) {
	t.Helper()
	out, err := exec.Command(bin, append([]string{"-hide_banner", "-loglevel", "error", "-y"}, args...)...).CombinedOutput()
	tk.True(t, err == nil, "ffmpeg %v: %v\n%s", args, err, out)
}

func TestFFmpegSmoke(t *testing.T) {
	bin := requireFFmpeg(t)
	dir := t.TempDir()
	good := filepath.Join(dir, "good.mp4")
	ffmpeg(t, bin, "-f", "lavfi", "-i", "testsrc=duration=2:size=640x360:rate=25", "-pix_fmt", "yuv420p", good)
	audioOnly := filepath.Join(dir, "audio.mp4")
	ffmpeg(t, bin, "-f", "lavfi", "-i", "sine=duration=1", audioOnly)
	data, _ := os.ReadFile(good)
	truncated := filepath.Join(dir, "truncated.mp4")
	tk.Equal(t, os.WriteFile(truncated, data[:4000], 0o644), nil, "write truncated")
	garbage := filepath.Join(dir, "garbage.avi")
	tk.Equal(t, os.WriteFile(garbage, []byte("definitely not a video, just some bytes"), 0o644), nil, "write garbage")

	launch := adapters.FFmpegLauncher(bin, 320)
	runFFmpeg := func(input string, timeout time.Duration) (worker.ProcessResult, string) {
		out := filepath.Join(dir, filepath.Base(input)+".jpg")
		p, err := launch(input, out)
		tk.Equal(t, err, nil, "launch")
		return worker.RunProcess(context.Background(), p, timeout, 2*time.Second), out
	}

	t.Run("a valid video produces a 320px-wide JPEG", func(t *testing.T) {
		result, out := runFFmpeg(good, 30*time.Second)
		tk.Equal(t, [2]any{result.Outcome, result.ExitCode}, [2]any{worker.Exited, 0}, "result: "+result.Stderr)
		img, _ := os.ReadFile(out)
		contentType, ok := worker.DetectImageType(img)
		tk.True(t, ok && contentType == "image/jpeg", "output is %q", contentType)
	})

	for name, c := range map[string]struct {
		input string
		kind  worker.FailureKind
	}{
		"truncated mp4 (moov atom missing)": {truncated, worker.KindCorruptInput},
		"garbage bytes":                     {garbage, worker.KindCorruptInput},
		"audio-only file":                   {audioOnly, worker.KindNoVideoStream},
		"missing file":                      {filepath.Join(dir, "nope.mp4"), worker.KindInputUnavailable},
	} {
		t.Run("real stderr is classified: "+name, func(t *testing.T) {
			result, _ := runFFmpeg(c.input, 30*time.Second)
			tk.True(t, result.ExitCode != 0, "expected failure, got exit 0")
			f := worker.ClassifyProcessFailure(result.ExitCode, result.Stderr)
			tk.Equal(t, f.Kind, c.kind, "kind for stderr:\n"+result.Stderr)
			tk.Equal(t, f.Retryable, false, "retryable")
		})
	}

	t.Run("a genuinely hung ffmpeg is killed at the timeout", func(t *testing.T) {
		fifo := filepath.Join(dir, "hang.mp4")
		tk.Equal(t, syscall.Mkfifo(fifo, 0o644), nil, "mkfifo") // ffmpeg blocks forever opening a FIFO with no writer
		start := time.Now()
		result, _ := runFFmpeg(fifo, 500*time.Millisecond)
		tk.Equal(t, [2]any{result.Outcome, result.Reaped}, [2]any{worker.TimedOut, true}, "result")
		tk.True(t, time.Since(start) < 5*time.Second, "took %s", time.Since(start))
	})

	t.Run("a missing ffmpeg binary fails to launch", func(t *testing.T) {
		_, err := adapters.FFmpegLauncher(filepath.Join(dir, "no-ffmpeg"), 320)(good, filepath.Join(dir, "x.jpg"))
		tk.True(t, err != nil, "expected a launch error")
	})
}

// TestEndToEnd runs the real service (real fs, ffmpeg, SQLite, blob dir) over a real directory twice.
func TestEndToEnd(t *testing.T) {
	bin := requireFFmpeg(t)
	root, data := t.TempDir(), t.TempDir()
	tk.Equal(t, os.MkdirAll(filepath.Join(root, "nested"), 0o755), nil, "mkdir")
	ffmpeg(t, bin, "-f", "lavfi", "-i", "testsrc=duration=1:size=320x240:rate=25", filepath.Join(root, "a.mp4"))
	ffmpeg(t, bin, "-f", "lavfi", "-i", "testsrc2=duration=1:size=320x240:rate=25", filepath.Join(root, "nested", "b.mov"))
	write(t, filepath.Join(root, "broken.mkv"), "not a video")
	write(t, filepath.Join(root, "notes.txt"), "hello")

	opts, err := app.ParseFlags([]string{"-root", root, "-data-dir", data, "-once", "-concurrency", "2"}, io.Discard)
	tk.Equal(t, err, nil, "flags")
	repo, err := adapters.OpenSQLite(opts.Paths().Database)
	tk.Equal(t, err, nil, "open db")
	defer repo.Close()

	runOnce := func() (*queue.Memory[domain.VideoJob], *metrics.Counters) {
		q := queue.NewMemory[domain.VideoJob](opts.QueueCapacity)
		m := metrics.New()
		svc := app.NewService(app.Deps{
			FS: adapters.OSFileSystem{}, Videos: q, Dirs: queue.NewUnbounded[scanner.DirJob](), Launch: adapters.FFmpegLauncher(bin, 320),
			Scratch: adapters.ScratchDir{Dir: opts.Paths().Scratch}, Blobs: adapters.LocalBlobStore{Root: opts.Paths().Blobs},
			Repository: repo, Events: app.LogEvents{Logger: quiet()},
			Random: tk.Seeded(1), Logger: quiet(), Metrics: m,
		}, opts.Service)
		tk.Equal(t, svc.Run(context.Background()), nil, "run")
		return q, m
	}

	q, m := runOnce()
	tk.Equal(t, m.Get("worker.generated"), 2, "generated")
	tk.Equal(t, len(q.DeadLetters()), 1, "dead letters (broken.mkv)")
	blobs := 0
	_ = filepath.WalkDir(opts.Paths().Blobs, func(_ string, d os.DirEntry, _ error) error {
		if d != nil && !d.IsDir() {
			blobs++
		}
		return nil
	})
	tk.Equal(t, blobs, 2, "thumbnails on disk")
	scratch, _ := os.ReadDir(opts.Paths().Scratch)
	tk.Equal(t, len(scratch), 0, "scratch files left")

	_, m = runOnce()
	tk.Equal(t, m.Get("worker.processes_started"), 0, "second run starts no ffmpeg")
	tk.Equal(t, m.Get("scanner.skipped.up_to_date"), 3, "second run skips everything")
}
