package worker

import (
	"context"
	"time"

	"github.com/vukkremic/thumbnailer/internal/domain"
)

type ProcessOutcome int

const (
	Exited ProcessOutcome = iota
	TimedOut
	Aborted
)

type ProcessResult struct {
	Outcome  ProcessOutcome
	ExitCode int // valid when Outcome is Exited
	Stderr   string
	// Reaped is false when the process ignored every kill. We stop waiting, so the worker slot
	// isn't stuck forever, and report it so it can be alerted on.
	Reaped bool
}

// killAttempts is the number of Kill calls before giving up (the adapter escalates SIGTERM → SIGKILL).
const killAttempts = 2

// RunProcess waits for p with a time limit. On timeout, or when ctx is cancelled (shutdown), it
// kills the process and confirms it exited.
func RunProcess(ctx context.Context, p domain.Process, timeout, killGrace time.Duration) ProcessResult {
	exited := make(chan int, 1)
	go func() {
		code, err := p.Wait()
		if err != nil {
			code = -1 // the wrapper itself failed; treat it like a crashed process
		}
		exited <- code
	}()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	var outcome ProcessOutcome
	select {
	case code := <-exited:
		return ProcessResult{Outcome: Exited, ExitCode: code, Stderr: p.Stderr(), Reaped: true}
	case <-timer.C:
		outcome = TimedOut
	case <-ctx.Done():
		outcome = Aborted
	}

	reaped := false
	for range killAttempts {
		p.Kill()
		grace := time.NewTimer(killGrace)
		select {
		case <-exited:
			reaped = true
		case <-grace.C:
		}
		grace.Stop()
		if reaped {
			break
		}
	}
	return ProcessResult{Outcome: outcome, Stderr: p.Stderr(), Reaped: reaped}
}
