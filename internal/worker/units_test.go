package worker_test

import (
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/vukkremic/thumbnailer/internal/storage"
	tk "github.com/vukkremic/thumbnailer/internal/testkit"
	"github.com/vukkremic/thumbnailer/internal/worker"
)

func TestClassifyProcessFailure(t *testing.T) {
	// Real ffmpeg stderr samples (the smoke tests check these against a live ffmpeg).
	permanent := map[string]worker.FailureKind{
		"[in#0 @ 0x1] moov atom not found\nError opening input: Invalid data found when processing input": worker.KindCorruptInput,
		"Error opening input files: Invalid data found when processing input":                             worker.KindCorruptInput,
		"[mov,mp4] could not find codec parameters for stream 0":                                          worker.KindCorruptInput,
		"[out#0/image2 @ 0x1] Output file does not contain any stream":                                    worker.KindNoVideoStream,
		"Stream map '0:v' matches no streams.":                                                            worker.KindNoVideoStream,
		"Decoder (codec hevc) not found for input stream #0:0":                                            worker.KindUnsupportedCodec,
		"/videos/a.mp4: No such file or directory":                                                        worker.KindInputUnavailable,
		"/videos/a.mp4: Permission denied":                                                                worker.KindInputUnavailable,
	}
	for stderr, kind := range permanent {
		f := worker.ClassifyProcessFailure(1, stderr)
		tk.Equal(t, f.Kind, kind, stderr)
		tk.Equal(t, f.Retryable, false, "retryable: "+stderr)
	}

	transient := []struct {
		code   int
		stderr string
	}{{1, tk.TransientStderr}, {137, ""}, {-1, "wrapper error"}, {234, "Conversion failed!"}}
	for _, c := range transient {
		f := worker.ClassifyProcessFailure(c.code, c.stderr)
		tk.Equal(t, f.Kind, worker.KindProcessError, c.stderr)
		tk.Equal(t, f.Retryable, true, "retryable: "+c.stderr)
	}

	tk.Equal(t, worker.ClassifyProcessFailure(137, "").Message, "ffmpeg exited with code 137", "empty stderr message")

	long := strings.Repeat("noise\n", 2000) + "the real error"
	summary := worker.SummarizeStderr(long)
	tk.True(t, len(summary) <= 2004 && strings.HasSuffix(summary, "the real error"), "summary keeps the tail: %q…", summary[:20])
}

func TestBackoff(t *testing.T) {
	b := worker.Backoff{Base: time.Second, Max: 30 * time.Second}
	random := tk.Seeded(1)

	for attempt := 1; attempt <= 5; attempt++ {
		ceiling := time.Second << (attempt - 1)
		for range 100 {
			d := b.Delay(attempt, random)
			tk.True(t, d >= ceiling/2 && d <= ceiling, "attempt %d: delay %s outside [%s, %s]", attempt, d, ceiling/2, ceiling)
		}
	}

	tk.Equal(t, b.Delay(50, func() float64 { return 0 }), 15*time.Second, "capped (min)")
	tk.True(t, b.Delay(50, func() float64 { return 0.9999 }) <= 30*time.Second, "capped (max)")

	distinct := map[time.Duration]bool{}
	for range 50 {
		distinct[b.Delay(3, random)] = true
	}
	tk.True(t, len(distinct) > 40, "jitter should spread retries, got %d distinct delays", len(distinct))
}

func TestDetectImageType(t *testing.T) {
	valid := map[string][]byte{
		"image/jpeg": {0xff, 0xd8, 0xff, 0xe0},
		"image/png":  {0x89, 'P', 'N', 'G', 0x0d, 0x0a, 0x1a, 0x0a, 0},
		"image/webp": []byte("RIFF\x00\x00\x00\x00WEBPVP8 "),
	}
	for want, data := range valid {
		got, ok := worker.DetectImageType(data)
		tk.True(t, ok && got == want, "%v: got %q", data, got)
	}
	for _, data := range [][]byte{nil, {0xff}, {0xff, 0xd8}, {0, 1, 2, 3}, []byte("<html>")} {
		_, ok := worker.DetectImageType(data)
		tk.True(t, !ok, "%v should not be an image", data)
	}
}

func TestBlobKey(t *testing.T) {
	tk.Equal(t, storage.BlobKey("/a.mp4", "v1", "image/jpeg"), storage.BlobKey("/a.mp4", "v1", "image/jpeg"), "deterministic")
	tk.True(t, storage.BlobKey("/a.mp4", "v1", "image/jpeg") != storage.BlobKey("/a.mp4", "v2", "image/jpeg"), "changes with version")
	key := storage.BlobKey("/weird/../path with spaces/ünïcode.mp4", "v1", "image/jpeg")
	tk.True(t, regexp.MustCompile(`^thumbnails/[0-9a-f]{2}/[0-9a-f]{32}/[0-9a-f]{16}\.jpg$`).MatchString(key), "key %q", key)
}
