package loadtest

import (
	"fmt"
	"io/fs"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vukkremic/thumbnailer/internal/domain"
)

var jpeg = []byte{0xff, 0xd8, 0xff, 0xe0, 0x00, 0x10, 'J', 'F', 'I', 'F', 0x00, 0xff, 0xd9}

// Launcher is an in-process stand-in for ffmpeg. Each video's behavior comes from GeneratedFS,
// and "decoding" is a sleep of Work. It runs in-process because spawning millions of real
// processes would measure fork/exec instead of the pipeline.
type Launcher struct {
	FS      *GeneratedFS
	Scratch *MemScratch
	Work    time.Duration
	flaky   sync.Map // path → *atomic.Int32 attempts so far

	running atomic.Int64
	peak    atomic.Int64
}

func (l *Launcher) Launch(videoPath, outputPath string) (domain.Process, error) {
	p := &fakeProc{done: make(chan struct{}), killed: make(chan struct{})}
	n := l.running.Add(1)
	for {
		old := l.peak.Load()
		if n <= old || l.peak.CompareAndSwap(old, n) {
			break
		}
	}
	go func() {
		defer l.running.Add(-1)
		p.run(l, videoPath, outputPath)
	}()
	return p, nil
}

func (l *Launcher) PeakRunning() int64 { return l.peak.Load() }

type fakeProc struct {
	once   sync.Once
	done   chan struct{}
	killed chan struct{}
	code   int
	stderr string
}

func (p *fakeProc) run(l *Launcher, videoPath, outputPath string) {
	defer close(p.done)
	work := l.Work
	switch l.FS.Behavior(videoPath) {
	case Corrupt:
		p.code, p.stderr = p.after(work, 1, "[mov,mp4 @ 0x1] moov atom not found\nInvalid data found when processing input")
		return
	case Hang:
		<-p.killed
		p.code = 137
		return
	case Slow:
		work *= 50
	case Flaky:
		counter, _ := l.flaky.LoadOrStore(videoPath, new(atomic.Int32))
		if counter.(*atomic.Int32).Add(1) <= 2 {
			p.code, p.stderr = p.after(work, 1, "av_interleaved_write_frame(): Input/output error")
			return
		}
	}
	if p.code, p.stderr = p.after(work, 0, ""); p.code == 0 {
		l.Scratch.write(outputPath, jpeg)
	}
}

// after waits d (or until killed) and returns the given exit code, or 137 if killed.
func (p *fakeProc) after(d time.Duration, code int, stderr string) (int, string) {
	if d > 0 {
		t := time.NewTimer(d)
		defer t.Stop()
		select {
		case <-t.C:
		case <-p.killed:
			return 137, ""
		}
	}
	return code, stderr
}

func (p *fakeProc) Kill() { p.once.Do(func() { close(p.killed) }) }

func (p *fakeProc) Wait() (int, error) {
	<-p.done
	return p.code, nil
}

func (p *fakeProc) Stderr() string {
	select {
	case <-p.done:
		return p.stderr
	default:
		return ""
	}
}

// MemScratch is an in-memory domain.ScratchSpace; it only ever holds in-flight outputs.
type MemScratch struct {
	mu    sync.Mutex
	files map[string][]byte
	next  atomic.Int64
}

func NewMemScratch() *MemScratch { return &MemScratch{files: map[string][]byte{}} }

func (s *MemScratch) Allocate(ext string) (string, error) {
	return fmt.Sprintf("/scratch/%d%s", s.next.Add(1), ext), nil
}

func (s *MemScratch) Read(p string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, ok := s.files[p]
	if !ok {
		return nil, &fs.PathError{Op: "read", Path: p, Err: fs.ErrNotExist}
	}
	return data, nil
}

func (s *MemScratch) Remove(p string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.files, p)
	return nil
}

func (s *MemScratch) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.files)
}

func (s *MemScratch) write(p string, data []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.files[p] = data
}
