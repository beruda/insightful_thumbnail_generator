package worker

import (
	"bytes"
	"time"
)

type Backoff struct {
	Base, Max time.Duration
}

// Delay is exponential backoff with "equal jitter": half the delay is fixed and half random.
// That keeps a minimum spacing between attempts while spreading out retries of jobs that failed
// together. Attempt 1 → [base/2, base], attempt 2 → [base, 2·base], …, capped at Max.
// random must return a value in [0, 1).
func (b Backoff) Delay(attempt int, random func() float64) time.Duration {
	ceiling := b.Base
	for i := 1; i < attempt && ceiling < b.Max; i++ {
		ceiling *= 2
	}
	ceiling = min(ceiling, b.Max)
	half := ceiling / 2
	return half + time.Duration(random()*float64(half))
}

// DetectImageType identifies an image by its magic bytes. Used to reject output that ffmpeg
// reported as successful but that isn't a usable image (truncated, empty, or garbage).
func DetectImageType(data []byte) (contentType string, ok bool) {
	switch {
	case bytes.HasPrefix(data, []byte{0xff, 0xd8, 0xff}):
		return "image/jpeg", true
	case bytes.HasPrefix(data, []byte{0x89, 'P', 'N', 'G', 0x0d, 0x0a, 0x1a, 0x0a}):
		return "image/png", true
	case len(data) >= 12 && bytes.Equal(data[:4], []byte("RIFF")) && bytes.Equal(data[8:12], []byte("WEBP")):
		return "image/webp", true
	}
	return "", false
}
