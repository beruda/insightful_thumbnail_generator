package adapters

import (
	"errors"
	"fmt"
	"os/exec"
	"sync"
	"syscall"

	"github.com/vukkremic/thumbnailer/internal/domain"
)

const maxStderrBytes = 64 * 1024

// FFmpegLauncher is the real implementation of the brief's startThumbnailProcess.
//
// The thumbnail filter picks the most representative of the first 50 frames. That skips black
// intro frames, still works for clips shorter than any fixed seek offset, and keeps decoding cost
// bounded no matter how long the video is.
func FFmpegLauncher(ffmpegPath string, width int) domain.Launcher {
	return func(videoPath, outputPath string) (domain.Process, error) {
		cmd := exec.Command(ffmpegPath,
			"-hide_banner", "-nostdin", "-loglevel", "error", "-y",
			"-i", videoPath,
			"-vf", fmt.Sprintf("thumbnail=50,scale=%d:-2", width),
			"-frames:v", "1", "-f", "image2", "-c:v", "mjpeg", "-q:v", "4",
			outputPath,
		)
		stderr := &tailBuffer{limit: maxStderrBytes}
		cmd.Stderr = stderr
		if err := cmd.Start(); err != nil {
			return nil, fmt.Errorf("start ffmpeg: %w", err)
		}
		p := &ffmpegProcess{cmd: cmd, stderr: stderr, done: make(chan struct{})}
		go p.wait()
		return p, nil
	}
}

type ffmpegProcess struct {
	cmd    *exec.Cmd
	stderr *tailBuffer
	done   chan struct{}
	code   int
	err    error
	mu     sync.Mutex
	kills  int
}

func (p *ffmpegProcess) wait() {
	err := p.cmd.Wait()
	var exitErr *exec.ExitError
	switch {
	case err == nil:
		p.code = 0
	case errors.As(err, &exitErr):
		p.code = exitErr.ExitCode()
		if status, ok := exitErr.Sys().(syscall.WaitStatus); ok && status.Signaled() {
			p.code = 128 + int(status.Signal()) // like a shell reports it
		}
	default:
		p.err = err
	}
	close(p.done)
}

// Kill asks nicely first (SIGTERM), then forces it (SIGKILL).
func (p *ffmpegProcess) Kill() {
	p.mu.Lock()
	defer p.mu.Unlock()
	sig := syscall.SIGTERM
	if p.kills > 0 {
		sig = syscall.SIGKILL
	}
	p.kills++
	_ = p.cmd.Process.Signal(sig)
}

func (p *ffmpegProcess) Wait() (int, error) {
	<-p.done
	return p.code, p.err
}

func (p *ffmpegProcess) Stderr() string { return p.stderr.String() }

// tailBuffer keeps only the last `limit` bytes. That's where ffmpeg reports errors, and it bounds memory.
type tailBuffer struct {
	mu    sync.Mutex
	limit int
	buf   []byte
}

func (b *tailBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf = append(b.buf, p...)
	if over := len(b.buf) - b.limit; over > 0 {
		b.buf = b.buf[over:]
	}
	return len(p), nil
}

func (b *tailBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return string(b.buf)
}
