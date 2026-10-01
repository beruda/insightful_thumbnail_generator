// Package loadtest runs the real service against a synthetic file system of any size (millions
// of files, no disk use, no setup or cleanup), an in-process fake ffmpeg, and optional injected
// latency to mimic an object store. It reports throughput and resource peaks.
package loadtest

import (
	"context"
	"fmt"
	"io/fs"
	"path"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/vukkremic/thumbnailer/internal/domain"
)

// TreeConfig describes a generated tree. Directories form a complete FanOut-ary tree (heap
// layout: the children of directory i are FanOut*i+1 … FanOut*i+FanOut). Each directory holds
// FilesPerDir files, until Files files exist in total.
type TreeConfig struct {
	Seed        uint64
	Files       int
	FanOut      int
	FilesPerDir int
	// VideoRatio is the fraction of files with a video extension; the rest are .txt.
	VideoRatio float64
	Mix        Mix
}

// Mix is the fraction of videos that misbehave in each way; the rest succeed.
type Mix struct {
	Corrupt float64 // fails permanently (moov atom not found)
	Slow    float64 // takes 50× the normal work time
	Hang    float64 // never exits until killed
	Flaky   float64 // fails twice with a transient error, then succeeds
}

type BehaviorKind string

const (
	OK      BehaviorKind = "ok"
	Corrupt BehaviorKind = "corrupt"
	Slow    BehaviorKind = "slow"
	Hang    BehaviorKind = "hang"
	Flaky   BehaviorKind = "flaky"
)

// modTime is fixed so a second pass sees every video as unchanged.
var modTime = time.UnixMilli(1_700_000_000_000)

// GeneratedFS is a domain.FileSystem whose contents are computed from the path and a seed, so a
// tree of any size costs no memory or disk. It is deterministic: same config, same tree.
type GeneratedFS struct {
	root    string
	cfg     TreeConfig
	numDirs int
}

func NewGeneratedFS(root string, cfg TreeConfig) *GeneratedFS {
	if cfg.FanOut < 2 {
		cfg.FanOut = 2
	}
	if cfg.FilesPerDir < 1 {
		cfg.FilesPerDir = 1
	}
	numDirs := max(1, (cfg.Files+cfg.FilesPerDir-1)/cfg.FilesPerDir)
	return &GeneratedFS{root: strings.TrimSuffix(root, "/"), cfg: cfg, numDirs: numDirs}
}

func (g *GeneratedFS) Root() string { return g.root }
func (g *GeneratedFS) Dirs() int    { return g.numDirs }

func (g *GeneratedFS) ListFiles(_ context.Context, p string) ([]string, error) {
	dir, ok := g.parseDir(p)
	if !ok {
		return nil, notExist(p)
	}
	var out []string
	for c := g.cfg.FanOut*dir + 1; c <= g.cfg.FanOut*dir+g.cfg.FanOut && c < g.numDirs; c++ {
		out = append(out, p+"/d"+strconv.Itoa(c))
	}
	for j := range g.filesIn(dir) {
		out = append(out, p+"/"+g.fileName(dir, j))
	}
	return out, nil
}

func (g *GeneratedFS) OpenFile(_ context.Context, p string) (domain.FileInfo, error) {
	if _, ok := g.parseDir(p); ok {
		return domain.FileInfo{Path: p, Name: path.Base(p), Permissions: 0o755, IsDir: true, CanonicalPath: p, ModTime: modTime}, nil
	}
	dir, j, ok := g.parseFile(p)
	if !ok {
		return domain.FileInfo{}, notExist(p)
	}
	h := g.hash(dir, j)
	return domain.FileInfo{
		Path:          p,
		Name:          path.Base(p),
		Permissions:   0o644,
		Size:          1<<20 + int64(h%(500<<20)), // 1 MB … 501 MB
		CanonicalPath: p,
		ModTime:       modTime,
	}, nil
}

// Behavior returns how the fake ffmpeg should treat the video at p.
func (g *GeneratedFS) Behavior(p string) BehaviorKind {
	dir, j, ok := g.parseFile(p)
	if !ok {
		return Corrupt // not one of ours; ffmpeg would fail to open it
	}
	r := float64(g.hash(dir, j)>>11&(1<<40-1)) / float64(uint64(1)<<40)
	m := g.cfg.Mix
	switch {
	case r < m.Corrupt:
		return Corrupt
	case r < m.Corrupt+m.Slow:
		return Slow
	case r < m.Corrupt+m.Slow+m.Hang:
		return Hang
	case r < m.Corrupt+m.Slow+m.Hang+m.Flaky:
		return Flaky
	}
	return OK
}

// Census walks the whole tree in memory to count videos per behavior (for validating a run).
func (g *GeneratedFS) Census() (files, videos int, byKind map[BehaviorKind]int) {
	byKind = map[BehaviorKind]int{}
	for dir := range g.numDirs {
		for j := range g.filesIn(dir) {
			files++
			if g.isVideo(dir, j) {
				videos++
				byKind[g.Behavior(g.dirPath(dir)+"/"+g.fileName(dir, j))]++
			}
		}
	}
	return files, videos, byKind
}

func (g *GeneratedFS) filesIn(dir int) int {
	start := dir * g.cfg.FilesPerDir
	if start >= g.cfg.Files {
		return 0
	}
	return min(g.cfg.FilesPerDir, g.cfg.Files-start)
}

// fileName is f<dir>_<j>.<ext>, unique across the tree.
func (g *GeneratedFS) fileName(dir, j int) string {
	ext := "txt"
	if g.isVideo(dir, j) {
		ext = []string{"mp4", "mov", "mkv"}[g.hash(dir, j)%3]
	}
	return fmt.Sprintf("f%d_%d.%s", dir, j, ext)
}

func (g *GeneratedFS) isVideo(dir, j int) bool {
	return float64(g.hash(dir, j)>>24&0xffff)/65536 < g.cfg.VideoRatio
}

func (g *GeneratedFS) dirPath(dir int) string {
	var parts []string
	for d := dir; d > 0; d = (d - 1) / g.cfg.FanOut {
		parts = append(parts, "d"+strconv.Itoa(d))
	}
	slices.Reverse(parts)
	if len(parts) == 0 {
		return g.root
	}
	return g.root + "/" + strings.Join(parts, "/")
}

func (g *GeneratedFS) parseDir(p string) (int, bool) {
	if p == g.root {
		return 0, true
	}
	base := path.Base(p)
	if !strings.HasPrefix(base, "d") {
		return 0, false
	}
	dir, err := strconv.Atoi(base[1:])
	if err != nil || dir <= 0 || dir >= g.numDirs || g.dirPath(dir) != p {
		return 0, false
	}
	return dir, true
}

func (g *GeneratedFS) parseFile(p string) (dir, j int, ok bool) {
	dir, ok = g.parseDir(path.Dir(p))
	if !ok {
		return 0, 0, false
	}
	name := path.Base(p)
	stem := strings.TrimSuffix(name, path.Ext(name))
	d, idx, found := strings.Cut(strings.TrimPrefix(stem, "f"), "_")
	if !found || d != strconv.Itoa(dir) {
		return 0, 0, false
	}
	j, err := strconv.Atoi(idx)
	if err != nil || j < 0 || j >= g.filesIn(dir) || g.fileName(dir, j) != name {
		return 0, 0, false
	}
	return dir, j, true
}

// hash is splitmix64 over (seed, global file index): cheap, well mixed, deterministic.
func (g *GeneratedFS) hash(dir, j int) uint64 {
	z := g.cfg.Seed + uint64(dir*g.cfg.FilesPerDir+j)*0x9e3779b97f4a7c15
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

func notExist(p string) error { return &fs.PathError{Op: "open", Path: p, Err: fs.ErrNotExist} }

// LatencyFS wraps a FileSystem, adds a fixed delay per call (to mimic an object store's
// round trips), and counts calls.
type LatencyFS struct {
	Inner       domain.FileSystem
	ListLatency time.Duration
	OpenLatency time.Duration
	lists       atomic.Int64
	opens       atomic.Int64
}

func (l *LatencyFS) ListFiles(ctx context.Context, p string) ([]string, error) {
	l.lists.Add(1)
	if err := sleep(ctx, l.ListLatency); err != nil {
		return nil, err
	}
	return l.Inner.ListFiles(ctx, p)
}

func (l *LatencyFS) OpenFile(ctx context.Context, p string) (domain.FileInfo, error) {
	l.opens.Add(1)
	if err := sleep(ctx, l.OpenLatency); err != nil {
		return domain.FileInfo{}, err
	}
	return l.Inner.OpenFile(ctx, p)
}

func (l *LatencyFS) Calls() (lists, opens int64) { return l.lists.Load(), l.opens.Load() }

func sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
