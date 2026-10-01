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
	"slices"
	"sort"
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

// FS is an in-memory domain.FileSystem with paginated listing. Faults (per-path errors, transient
// list failures, latency) can be injected, and calls and peak concurrency are recorded.
type FS struct {
	mu        sync.Mutex
	root      *node
	pageSize  int
	listErrs  map[string]listFault
	openErrs  map[string]error
	latency   time.Duration
	listCalls map[string]int // per directory

	openCalls, inFlight, peakInFlight int
}

type listFault struct {
	err   error
	times int // remaining failures; < 0 means forever
}

func NewFS() *FS {
	return &FS{root: newDir(), pageSize: 1000, listErrs: map[string]listFault{}, openErrs: map[string]error{}, listCalls: map[string]int{}}
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

func (f *FS) Dir(p string) *FS { return f.put(p, newDir()) }

// Symlink adds a symbolic link entry. Its target doesn't matter: scanners skip symlinks.
func (f *FS) Symlink(p string) *FS { return f.put(p, &node{kind: kindSymlink, perm: 0o777}) }

// WithPageSize sets how many entries one ListPage call returns.
func (f *FS) WithPageSize(n int) *FS {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pageSize = n
	return f
}

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
	if parent := f.lookup(p, true); parent != nil && parent.kind == kindDir {
		delete(parent.children, path.Base(p))
	}
}

// FailList makes every ListPage(dir) fail with err (ErrIO if nil).
func (f *FS) FailList(dir string, err error) *FS { return f.FailListTimes(dir, -1, err) }

// FailListTimes makes the next n ListPage(dir) calls fail with err (ErrIO if nil).
func (f *FS) FailListTimes(dir string, n int, err error) *FS {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.listErrs[dir] = listFault{err: cmpOr(err, ErrIO), times: n}
	return f
}

// FailOpen makes OpenFile(p) fail with err (ErrIO if nil).
func (f *FS) FailOpen(p string, err error) *FS {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.openErrs[p] = cmpOr(err, ErrIO)
	return f
}

// WithLatency makes every call take d (of virtual time, inside a synctest bubble).
func (f *FS) WithLatency(d time.Duration) *FS {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.latency = d
	return f
}

// ListCalls returns the number of ListPage calls, in total or (with a dir) for one directory.
func (f *FS) ListCalls(dir ...string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for d, c := range f.listCalls {
		if len(dir) == 0 || d == dir[0] {
			n += c
		}
	}
	return n
}

func (f *FS) OpenCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.openCalls
}

func (f *FS) PeakInFlight() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.peakInFlight
}

func (f *FS) ListPage(_ context.Context, dir, token string) (domain.Page, error) {
	f.mu.Lock()
	f.listCalls[dir]++
	f.mu.Unlock()
	return track(f, func() (domain.Page, error) {
		if fault, ok := f.listErrs[dir]; ok && fault.times != 0 {
			if fault.times > 0 {
				fault.times--
				f.listErrs[dir] = fault
			}
			return domain.Page{}, fault.err
		}
		d := f.lookup(dir, false)
		switch {
		case d == nil:
			return domain.Page{}, NotExist(dir)
		case d.kind != kindDir:
			return domain.Page{}, fmt.Errorf("%s: not a directory", dir)
		}
		names := make([]string, 0, len(d.children))
		for name := range d.children {
			names = append(names, name)
		}
		slices.Sort(names)
		start := sort.SearchStrings(names, token)
		if start < len(names) && names[start] == token {
			start++
		}
		end := min(start+f.pageSize, len(names))
		page := domain.Page{}
		for _, name := range names[start:end] {
			page.Entries = append(page.Entries, info(path.Join(dir, name), d.children[name]))
		}
		if end < len(names) {
			page.NextToken = names[end-1]
		}
		return page, nil
	})
}

func (f *FS) OpenFile(_ context.Context, p string) (domain.FileInfo, error) {
	f.mu.Lock()
	f.openCalls++
	f.mu.Unlock()
	return track(f, func() (domain.FileInfo, error) {
		if err := f.openErrs[p]; err != nil {
			return domain.FileInfo{}, err
		}
		n := f.lookup(p, false)
		if n == nil {
			return domain.FileInfo{}, NotExist(p)
		}
		return info(p, n), nil
	})
}

func info(p string, n *node) domain.FileInfo {
	return domain.FileInfo{
		Path: p, Name: path.Base(p), Permissions: n.perm, Size: n.size, ModTime: n.modTime, ETag: n.etag,
		IsDir: n.kind == kindDir, IsSymlink: n.kind == kindSymlink,
	}
}

// track runs op under the lock after the configured latency, recording peak concurrency.
func track[T any](f *FS, op func() (T, error)) (T, error) {
	f.mu.Lock()
	f.inFlight++
	f.peakInFlight = max(f.peakInFlight, f.inFlight)
	latency := f.latency
	f.mu.Unlock()

	if latency > 0 {
		time.Sleep(latency)
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	f.inFlight--
	return op()
}

// lookup finds the node at p (or its parent, if parent is true). The caller holds f.mu.
func (f *FS) lookup(p string, parent bool) *node {
	parts := split(p)
	if parent {
		if len(parts) == 0 {
			return nil
		}
		parts = parts[:len(parts)-1]
	}
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
