package scanner

import (
	"context"
	"errors"
	"io/fs"
	"iter"
	"log/slog"
	"slices"
	"sync"

	"github.com/vukkremic/thumbnailer/internal/domain"
	"github.com/vukkremic/thumbnailer/internal/metrics"
)

// Position is the walk state that gets saved as a checkpoint. It is captured right after a file
// is yielded, so resuming from it continues with the *next* file: nothing is missed, and nothing
// repeats as long as a checkpoint is saved after each consumed file.
type Position struct {
	// Pending directories still to be listed, used as a stack: the last one is listed next.
	Pending []string `json:"pending"`
	// Current is the directory being iterated, if any.
	Current *Cursor `json:"current,omitempty"`
}

type Cursor struct {
	Dir string `json:"dir"`
	// After is the last child (in sorted order) already handled; empty means none yet.
	After string `json:"after,omitempty"`
}

type WalkerOptions struct {
	// Concurrency caps parallel OpenFile calls (they dominate on remote file systems).
	Concurrency int
	// FollowSymlinks follows symlinked directories and files. Loops are detected via canonical paths.
	FollowSymlinks bool
	Logger         *slog.Logger
	Metrics        *metrics.Counters
}

// Walker does an iterative (not recursive) depth-first walk and yields files lazily:
//   - memory holds only the stack of pending directories plus one chunk of entries, so huge trees
//     don't exhaust memory and deep trees can't overflow the stack;
//   - backpressure is automatic: nothing is listed or opened until the consumer asks for the next file;
//   - errors are isolated: a directory that can't be listed, or a file that can't be opened, is
//     logged and skipped;
//   - children are sorted so the order is deterministic, which resuming from a checkpoint relies on.
type Walker struct {
	fs      domain.FileSystem
	opts    WalkerOptions
	pending []string
	current *Cursor
	// visited holds canonical paths of visited directories, only tracked when following symlinks.
	visited map[string]bool
	err     error
}

// NewWalker starts a walk at root.
func NewWalker(fsys domain.FileSystem, root string, opts WalkerOptions) *Walker {
	return ResumeWalker(fsys, Position{Pending: []string{root}}, opts)
}

// ResumeWalker continues a walk from a saved position.
func ResumeWalker(fsys domain.FileSystem, pos Position, opts WalkerOptions) *Walker {
	if opts.Concurrency < 1 {
		opts.Concurrency = 1
	}
	w := &Walker{fs: fsys, opts: opts, pending: slices.Clone(pos.Pending), visited: map[string]bool{}}
	if pos.Current != nil {
		c := *pos.Current
		w.current = &c
	}
	return w
}

// Position returns the current walk state (safe to persist).
func (w *Walker) Position() Position {
	pos := Position{Pending: slices.Clone(w.pending)}
	if w.current != nil {
		c := *w.current
		pos.Current = &c
	}
	return pos
}

// Err returns the error that stopped the walk early (context cancellation), if any.
func (w *Walker) Err() error { return w.err }

// Files yields every file (not directory) under the walk's roots.
func (w *Walker) Files(ctx context.Context) iter.Seq[domain.FileInfo] {
	return func(yield func(domain.FileInfo) bool) {
		if w.opts.FollowSymlinks {
			w.markRootsVisited(ctx)
		}
		for {
			if err := ctx.Err(); err != nil {
				w.err = err
				return
			}
			if w.current == nil {
				if len(w.pending) == 0 {
					return
				}
				next := w.pending[len(w.pending)-1]
				w.pending = w.pending[:len(w.pending)-1]
				w.current = &Cursor{Dir: next}
			}
			if !w.walkDirectory(ctx, w.current, yield) {
				return
			}
			w.current = nil
		}
	}
}

// walkDirectory returns false if the walk must stop (the consumer stopped or ctx was cancelled).
func (w *Walker) walkDirectory(ctx context.Context, cursor *Cursor, yield func(domain.FileInfo) bool) bool {
	children, err := w.fs.ListFiles(ctx, cursor.Dir)
	if err != nil {
		if ctx.Err() != nil {
			w.err = ctx.Err()
			return false
		}
		w.opts.Metrics.Inc("scanner.list_errors")
		w.opts.Logger.Warn("cannot list directory, skipping", "dir", cursor.Dir, "error", err)
		return true
	}
	slices.Sort(children)
	if cursor.After != "" {
		start, _ := slices.BinarySearch(children, cursor.After)
		for start < len(children) && children[start] <= cursor.After {
			start++
		}
		children = children[start:]
	}

	for chunk := range slices.Chunk(children, w.opts.Concurrency) {
		if err := ctx.Err(); err != nil {
			w.err = err
			return false
		}
		infos, errs := w.openAll(ctx, chunk)
		for i, child := range chunk {
			switch {
			case errs[i] != nil:
				w.reportOpenError(child, errs[i])
			case infos[i].IsDir:
				if w.shouldDescend(infos[i]) {
					w.pending = append(w.pending, child)
				}
			case !infos[i].IsSymlink || w.opts.FollowSymlinks:
				// Advance the cursor *before* yielding, so a checkpoint taken while the consumer
				// handles this file resumes after it.
				cursor.After = child
				if !yield(infos[i]) {
					return false
				}
				continue
			}
			cursor.After = child
		}
	}
	return true
}

func (w *Walker) openAll(ctx context.Context, paths []string) ([]domain.FileInfo, []error) {
	infos := make([]domain.FileInfo, len(paths))
	errs := make([]error, len(paths))
	var wg sync.WaitGroup
	for i, p := range paths {
		wg.Go(func() { infos[i], errs[i] = w.fs.OpenFile(ctx, p) })
	}
	wg.Wait()
	return infos, errs
}

func (w *Walker) shouldDescend(dir domain.FileInfo) bool {
	if !w.opts.FollowSymlinks {
		return !dir.IsSymlink
	}
	if w.visited[dir.CanonicalPath] {
		w.opts.Metrics.Inc("scanner.symlink_loops")
		w.opts.Logger.Debug("directory already visited, skipping", "dir", dir.Path, "canonical", dir.CanonicalPath)
		return false
	}
	w.visited[dir.CanonicalPath] = true
	return true
}

func (w *Walker) markRootsVisited(ctx context.Context) {
	dirs := slices.Clone(w.pending)
	if w.current != nil {
		dirs = append(dirs, w.current.Dir)
	}
	for _, dir := range dirs {
		if info, err := w.fs.OpenFile(ctx, dir); err == nil {
			w.visited[info.CanonicalPath] = true
		} // On error, the directory fails again (and is reported) when it's listed.
	}
}

func (w *Walker) reportOpenError(path string, err error) {
	if errors.Is(err, fs.ErrNotExist) {
		// Deleted between listing and opening: normal churn on a live file system.
		w.opts.Metrics.Inc("scanner.vanished")
		w.opts.Logger.Debug("file vanished during scan", "path", path)
		return
	}
	w.opts.Metrics.Inc("scanner.open_errors")
	w.opts.Logger.Warn("cannot open file, skipping", "path", path, "error", err)
}
