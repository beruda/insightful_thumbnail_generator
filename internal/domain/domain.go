// Package domain holds the types and interfaces shared by the scanner and the worker.
// Every side effect sits behind one of these interfaces, so tests can swap in fakes.
package domain

import (
	"context"
	"fmt"
	"time"
)

// FileSystem is the brief's IFileSystem, adapted for scale:
//   - methods take a context and return errors, since a remote file system can be slow or fail;
//   - listing is paginated and returns each child's metadata, the way object stores do (S3's
//     ListObjectsV2 returns size, ETag and mtime for up to 1000 keys per call), so a scan needs
//     no per-file stat and one huge directory splits into many independent pages;
//   - FileInfo gains ModTime/ETag, to detect changed sources.
//
// Errors for missing paths must satisfy errors.Is(err, fs.ErrNotExist), as os errors do.
type FileSystem interface {
	// ListPage returns one page of dir's direct children. pageToken is "" for the first page;
	// the returned NextToken is "" on the last page.
	ListPage(ctx context.Context, dir, pageToken string) (Page, error)
	// OpenFile returns metadata for one path (the worker uses it to re-check a source).
	OpenFile(ctx context.Context, path string) (FileInfo, error)
}

type Page struct {
	Entries   []FileInfo
	NextToken string
}

type FileInfo struct {
	Path        string
	Name        string
	Permissions uint32 // POSIX mode bits, e.g. 0o644
	Size        int64
	IsDir       bool
	// IsSymlink marks a symbolic link. Scanners skip these: following them safely needs loop
	// detection shared across scanner replicas, and object stores don't have them anyway.
	IsSymlink bool
	ModTime   time.Time
	// ETag is the content version reported by object stores; preferred over ModTime when set.
	ETag string
}

// Process is the brief's IProcess.
type Process interface {
	// Kill terminates the process. Implementations should escalate on repeated calls
	// (SIGTERM, then SIGKILL), since a wedged process may ignore the first signal.
	Kill()
	// Wait blocks until the process exits and returns its exit code.
	Wait() (exitCode int, err error)
	// Stderr returns the process's stderr (complete once it has exited).
	Stderr() string
}

// Launcher is the brief's startThumbnailProcess. It fails if the process can't be started.
type Launcher func(videoPath, outputPath string) (Process, error)

// ScratchSpace is local disk where ffmpeg writes output before it's validated and uploaded.
type ScratchSpace interface {
	// Allocate returns a fresh, unused path. The file itself is not created.
	Allocate(extension string) (string, error)
	// Read returns the file's contents; errors.Is(err, fs.ErrNotExist) if it doesn't exist.
	Read(path string) ([]byte, error)
	// Remove deletes the file; a no-op if it doesn't exist.
	Remove(path string) error
}

// VideoJob is the message the scanner publishes for each video that needs a thumbnail.
type VideoJob struct {
	Path string `json:"path"`
	Size int64  `json:"size"`
	// SourceVersion identifies the content version; a new version needs a new thumbnail.
	SourceVersion string `json:"sourceVersion"`
}

// SourceVersion is the ETag if the file system has one, otherwise size + mtime.
func SourceVersion(f FileInfo) string {
	if f.ETag != "" {
		return f.ETag
	}
	return fmt.Sprintf("%d-%d", f.Size, f.ModTime.UnixMilli())
}

func JobFor(f FileInfo) VideoJob {
	return VideoJob{Path: f.Path, Size: f.Size, SourceVersion: SourceVersion(f)}
}
