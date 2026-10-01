package testkit

import (
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"sync"
	"time"

	"github.com/vukkremic/thumbnailer/internal/domain"
)

// JPEG is a minimal valid JPEG header: enough for magic-byte detection.
var JPEG = []byte{0xff, 0xd8, 0xff, 0xe0, 0x00, 0x10, 'J', 'F', 'I', 'F', 0x00, 0xff, 0xd9}

const (
	CorruptStderr   = "[mov,mp4 @ 0x1] moov atom not found\nInvalid data found when processing input"
	TransientStderr = "av_interleaved_write_frame(): Input/output error"
)

// Script says what one fake ffmpeg run does.
type Script struct {
	kind        string // "exit", "hang", "spawn_error"
	delay       time.Duration
	code        int
	stderr      string
	output      []byte
	noOutput    bool
	ignoreKills int
}

// Succeed exits 0 after 10ms, having written a JPEG.
func Succeed() Script { return Script{kind: "exit", delay: 10 * time.Millisecond, output: JPEG} }

// SucceedAfter exits 0 after d.
func SucceedAfter(d time.Duration) Script { s := Succeed(); s.delay = d; return s }

// SucceedWithOutput exits 0 but writes output (nil writes no file at all).
func SucceedWithOutput(output []byte) Script {
	s := Succeed()
	s.output, s.noOutput = output, output == nil
	return s
}

func ExitWith(code int, stderr string) Script {
	return Script{kind: "exit", delay: 10 * time.Millisecond, code: code, stderr: stderr}
}

// Hang never exits on its own; it ignores the first ignoreKills Kill calls (-1: unkillable).
func Hang(ignoreKills int) Script { return Script{kind: "hang", ignoreKills: ignoreKills} }

// SpawnError makes the launcher fail, as when the ffmpeg binary is missing.
func SpawnError() Script { return Script{kind: "spawn_error"} }

type ProcState string

const (
	Running    ProcState = "running"
	Exited     ProcState = "exited"
	Killed     ProcState = "killed"
	Unkillable ProcState = "unkillable"
)

// Launcher is a scriptable domain.Launcher. Scripts are set per video path and consumed one per
// attempt, with the last one repeating, so "fail twice, then succeed" is
// Script(path, ExitWith(1, ...), ExitWith(1, ...), Succeed()).
type Launcher struct {
	mu       sync.Mutex
	scratch  *Scratch
	scripts  map[string][]Script
	attempts map[string]int
	procs    []*Proc
	running  int
	peak     int
	Default  Script
}

func NewLauncher(scratch *Scratch) *Launcher {
	return &Launcher{scratch: scratch, scripts: map[string][]Script{}, attempts: map[string]int{}, Default: Succeed()}
}

func (l *Launcher) Script(videoPath string, scripts ...Script) *Launcher {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.scripts[videoPath] = scripts
	return l
}

func (l *Launcher) SetDefault(s Script) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.Default = s
}

func (l *Launcher) Launch(videoPath, outputPath string) (domain.Process, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	attempt := l.attempts[videoPath]
	l.attempts[videoPath]++
	script := l.Default
	if list := l.scripts[videoPath]; len(list) > 0 {
		script = list[min(attempt, len(list)-1)]
	}
	if script.kind == "spawn_error" {
		return nil, errors.New("exec: \"ffmpeg\": executable file not found in $PATH")
	}

	p := &Proc{l: l, VideoPath: videoPath, OutputPath: outputPath, state: Running, script: script,
		done: make(chan struct{}), killed: make(chan struct{})}
	l.procs = append(l.procs, p)
	l.running++
	l.peak = max(l.peak, l.running)
	if script.kind == "exit" {
		go p.run()
	}
	return p, nil
}

// Processes returns snapshots of every process started so far.
func (l *Launcher) Processes() []ProcSnapshot {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]ProcSnapshot, len(l.procs))
	for i, p := range l.procs {
		out[i] = ProcSnapshot{VideoPath: p.VideoPath, State: p.state, Kills: p.kills}
	}
	return out
}

func (l *Launcher) SpawnsFor(videoPath string) int {
	n := 0
	for _, p := range l.Processes() {
		if p.VideoPath == videoPath {
			n++
		}
	}
	return n
}

// Leaked returns processes still alive: never exited, or ignoring every kill.
func (l *Launcher) Leaked() []ProcSnapshot {
	var out []ProcSnapshot
	for _, p := range l.Processes() {
		if p.State == Running || p.State == Unkillable {
			out = append(out, p)
		}
	}
	return out
}

func (l *Launcher) PeakRunning() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.peak
}

// ForceExitAll ends every live process. Call it at the end of a test that leaves an unkillable
// process behind, so its goroutines can finish and the synctest bubble can exit.
func (l *Launcher) ForceExitAll() {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, p := range l.procs {
		p.finishLocked(Killed, 137)
	}
}

type ProcSnapshot struct {
	VideoPath string
	State     ProcState
	Kills     int
}

type Proc struct {
	l          *Launcher
	VideoPath  string
	OutputPath string
	script     Script
	state      ProcState // guarded by l.mu
	kills      int
	code       int
	done       chan struct{}
	killed     chan struct{}
}

func (p *Proc) run() {
	timer := time.NewTimer(p.script.delay)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-p.killed:
		return
	}
	p.l.mu.Lock()
	defer p.l.mu.Unlock()
	if p.state != Running {
		return
	}
	if p.script.code == 0 && !p.script.noOutput {
		p.l.scratch.write(p.OutputPath, p.script.output)
	}
	p.finishLocked(Exited, p.script.code)
}

func (p *Proc) Kill() {
	p.l.mu.Lock()
	defer p.l.mu.Unlock()
	p.kills++
	if p.state != Running && p.state != Unkillable {
		return
	}
	if p.script.ignoreKills < 0 || p.kills <= p.script.ignoreKills {
		p.state = Unkillable
		return
	}
	close(p.killed)
	p.finishLocked(Killed, 137)
}

func (p *Proc) finishLocked(state ProcState, code int) {
	if p.state != Running && p.state != Unkillable {
		return
	}
	p.state, p.code = state, code
	p.l.running--
	close(p.done)
}

func (p *Proc) Wait() (int, error) {
	<-p.done
	p.l.mu.Lock()
	defer p.l.mu.Unlock()
	return p.code, nil
}

func (p *Proc) Stderr() string {
	p.l.mu.Lock()
	defer p.l.mu.Unlock()
	if p.state == Exited {
		return p.script.stderr
	}
	return ""
}

// Scratch is an in-memory domain.ScratchSpace, so tests can assert no temp files are left behind.
type Scratch struct {
	mu    sync.Mutex
	files map[string][]byte
	next  int
}

func NewScratch() *Scratch { return &Scratch{files: map[string][]byte{}} }

func (s *Scratch) Allocate(ext string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.next++
	return fmt.Sprintf("/scratch/%d%s", s.next, ext), nil
}

func (s *Scratch) Read(p string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, ok := s.files[p]
	if !ok {
		return nil, &fs.PathError{Op: "read", Path: p, Err: fs.ErrNotExist}
	}
	return data, nil
}

func (s *Scratch) Remove(p string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.files, p)
	return nil
}

func (s *Scratch) Files() map[string][]byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return maps.Clone(s.files)
}

func (s *Scratch) write(p string, data []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.files[p] = data
}
