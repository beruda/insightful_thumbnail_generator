// Package testkit has in-memory fakes with fault injection, plus a harness that wires the whole
// service out of them. It is imported only by tests.
//
// Time-dependent tests run inside testing/synctest bubbles: time.Sleep, timers and contexts use a
// virtual clock that jumps forward whenever every goroutine is blocked, so a 2-minute timeout takes
// microseconds. And if every goroutine is blocked with no timer pending, synctest fails the test as
// a deadlock instead of hanging.
package testkit

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/vukkremic/thumbnailer/internal/domain"
)

type nodeKind int

const (
	kindDir nodeKind = iota
	kindFile
	kindSymlink
)

type node struct {
	kind     nodeKind
	size     int64
	modTime  time.Time
	perm     uint32
	etag     string
	target   string
	children map[string]*node
}

type FileOpt func(*node)

func Size(n int64) FileOpt       { return func(nd *node) { nd.size = n } }
func ModTimeMs(ms int64) FileOpt { return func(nd *node) { nd.modTime = time.UnixMilli(ms) } }
func Perm(p uint32) FileOpt      { return func(nd *node) { nd.perm = p } }
func ETag(tag string) FileOpt    { return func(nd *node) { nd.etag = tag } }

// ErrIO is the default injected failure for non-"not found" errors.
var ErrIO = errors.New("input/output error")

// NotExist returns an error satisfying errors.Is(err, fs.ErrNotExist), like os errors do.
func NotExist(p string) error { return &fs.PathError{Op: "open", Path: p, Err: fs.ErrNotExist} }

// FS is an in-memory domain.FileSystem. Faults (per-path errors, latency, files that vanish
// between list and open) can be injected, and calls and peak concurrency are recorded.
type FS struct {
	mu       sync.Mutex
	root     *node
	listErrs map[string]error
	openErrs map[string]error
	latency  time.Duration

	listCalls, openCalls, inFlight, peakInFlight int
}

func NewFS() *FS {
	return &FS{root: newDir(), listErrs: map[string]error{}, openErrs: map[string]error{}}
}

func newDir() *node { return &node{kind: kindDir, perm: 0o755, children: map[string]*node{}} }

// File adds a file (creating parent directories). Defaults: 1000 bytes, mtime 1000ms, mode 0644.
func (f *FS) File(p string, opts ...FileOpt) *FS {
	n := &node{kind: kindFile, size: 1000, modTime: time.UnixMilli(1000), perm: 0o644}
	for _, opt := range opts {
		opt(n)
	}
	return f.put(p, n)
}

func (f *FS) Dir(p string) *FS             { return f.put(p, newDir()) }
func (f *FS) Symlink(p, target string) *FS { return f.put(p, &node{kind: kindSymlink, target: target}) }

func (f *FS) put(p string, n *node) *FS {
	f.mu.Lock()
	defer f.mu.Unlock()
	parts := split(p)
	dir := f.root
	for _, part := range parts[:len(parts)-1] {
		child, ok := dir.children[part]
		if !ok {
			child = newDir()
			dir.children[part] = child
		}
		if child.kind != kindDir {
			panic(fmt.Sprintf("fakefs: %s is not a directory", part))
		}
		dir = child
	}
	dir.children[parts[len(parts)-1]] = n
	return f
}

func (f *FS) Remove(p string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if parent := f.lookup(split(path.Dir(p))); parent != nil && parent.kind == kindDir {
		delete(parent.children, path.Base(p))
	}
}

// FailList makes ListFiles(p) fail with err (ErrIO if nil).
func (f *FS) FailList(p string, err error) *FS {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.listErrs[p] = cmpOr(err, ErrIO)
	return f
}

// FailOpen makes OpenFile(p) fail with err (ErrIO if nil).
func (f *FS) FailOpen(p string, err error) *FS {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.openErrs[p] = cmpOr(err, ErrIO)
	return f
}

// VanishOnOpen: the file shows up in its directory listing but is gone by the time it's opened.
func (f *FS) VanishOnOpen(p string) *FS { return f.FailOpen(p, NotExist(p)) }

// WithLatency makes every call take d (of virtual time, inside a synctest bubble).
func (f *FS) WithLatency(d time.Duration) *FS {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.latency = d
	return f
}

func (f *FS) Calls() (list, open int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.listCalls, f.openCalls
}

func (f *FS) PeakInFlight() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.peakInFlight
}

func (f *FS) ListFiles(_ context.Context, p string) ([]string, error) {
	return track(f, &f.listCalls, f.listErrs, p, func() ([]string, error) {
		dir := f.lookup(split(f.canonical(p, 0)))
		switch {
		case dir == nil:
			return nil, NotExist(p)
		case dir.kind != kindDir:
			return nil, fmt.Errorf("%s: not a directory", p)
		}
		out := make([]string, 0, len(dir.children))
		for name := range dir.children { // map order: deliberately random, the walker must sort
			out = append(out, path.Join(p, name))
		}
		return out, nil
	})
}

func (f *FS) OpenFile(_ context.Context, p string) (domain.FileInfo, error) {
	return track(f, &f.openCalls, f.openErrs, p, func() (domain.FileInfo, error) {
		own := f.lookupNoFollow(p)
		if own == nil {
			return domain.FileInfo{}, NotExist(p)
		}
		canonical := f.canonical(p, 0)
		target := f.lookup(split(canonical))
		if target == nil {
			return domain.FileInfo{}, NotExist(p) // dangling symlink
		}
		info := domain.FileInfo{Path: p, Name: path.Base(p), IsSymlink: own.kind == kindSymlink, CanonicalPath: p}
		if info.IsSymlink {
			info.CanonicalPath = canonical
		}
		if target.kind == kindDir {
			info.IsDir, info.Permissions = true, 0o755
			return info, nil
		}
		info.Size, info.ModTime, info.Permissions, info.ETag = target.size, target.modTime, target.perm, target.etag
		return info, nil
	})
}

func track[T any](f *FS, counter *int, errs map[string]error, p string, op func() (T, error)) (T, error) {
	f.mu.Lock()
	*counter++
	f.inFlight++
	f.peakInFlight = max(f.peakInFlight, f.inFlight)
	latency, injected := f.latency, errs[p]
	f.mu.Unlock()

	if latency > 0 {
		time.Sleep(latency)
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	f.inFlight--
	if injected != nil {
		var zero T
		return zero, injected
	}
	return op()
}

// lookupNoFollow finds the node at p without following a symlink in the last segment.
func (f *FS) lookupNoFollow(p string) *node {
	parts := split(p)
	if len(parts) == 0 {
		return f.root
	}
	parent := f.lookup(split(f.canonical("/"+strings.Join(parts[:len(parts)-1], "/"), 0)))
	if parent == nil || parent.kind != kindDir {
		return nil
	}
	return parent.children[parts[len(parts)-1]]
}

// canonical resolves symlinks in every segment in one pass (O(depth) unless it crosses symlinks).
// Recursion is bounded, so cyclic links end the way ELOOP would on a real system.
func (f *FS) canonical(p string, depth int) string {
	if depth > 40 {
		return p
	}
	var out []string
	cur := f.root
	for _, part := range split(p) {
		var child *node
		if cur != nil && cur.kind == kindDir {
			child = cur.children[part]
		}
		if child != nil && child.kind == kindSymlink {
			out = split(f.canonical(child.target, depth+1))
			cur = f.lookup(out)
			continue
		}
		out = append(out, part)
		cur = child
	}
	return "/" + strings.Join(out, "/")
}

// lookup walks parts literally (no symlink resolution).
func (f *FS) lookup(parts []string) *node {
	cur := f.root
	for _, part := range parts {
		if cur.kind != kindDir {
			return nil
		}
		next, ok := cur.children[part]
		if !ok {
			return nil
		}
		cur = next
	}
	return cur
}

func split(p string) []string {
	var parts []string
	for _, s := range strings.Split(p, "/") {
		if s != "" {
			parts = append(parts, s)
		}
	}
	return parts
}

func cmpOr(err, fallback error) error {
	if err != nil {
		return err
	}
	return fallback
}
