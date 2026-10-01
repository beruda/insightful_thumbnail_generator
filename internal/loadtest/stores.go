package loadtest

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/vukkremic/thumbnailer/internal/domain"
	"github.com/vukkremic/thumbnailer/internal/storage"
)

// CompactRepository keeps only what the pipeline reads back (version, status, notified), so a
// million records cost tens of MB rather than hundreds and don't drown the scanner's own memory
// profile. Other fields are rebuilt on read.
type CompactRepository struct {
	mu      sync.RWMutex
	records map[string]compactRecord
	gets    atomic.Int64
	upserts atomic.Int64
}

type compactRecord struct {
	version  string
	failed   bool
	notified bool
}

func NewCompactRepository() *CompactRepository {
	return &CompactRepository{records: map[string]compactRecord{}}
}

func (r *CompactRepository) Get(_ context.Context, videoPath string) (domain.ThumbnailRecord, bool, error) {
	r.gets.Add(1)
	r.mu.RLock()
	c, ok := r.records[videoPath]
	r.mu.RUnlock()
	if !ok {
		return domain.ThumbnailRecord{}, false, nil
	}
	rec := domain.ThumbnailRecord{VideoPath: videoPath, SourceVersion: c.version, Status: domain.StatusReady, Notified: c.notified}
	if c.failed {
		rec.Status = domain.StatusFailed
	} else {
		rec.ContentType = "image/jpeg"
		rec.BlobKey = storage.BlobKey(videoPath, c.version, rec.ContentType)
	}
	return rec, true, nil
}

func (r *CompactRepository) Upsert(_ context.Context, rec domain.ThumbnailRecord) error {
	r.upserts.Add(1)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.records[rec.VideoPath] = compactRecord{version: rec.SourceVersion, failed: rec.Status == domain.StatusFailed, notified: rec.Notified}
	return nil
}

// Counts returns how many records are ready (and of those, notified) and failed.
func (r *CompactRepository) Counts() (ready, notified, failed int) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, c := range r.records {
		switch {
		case c.failed:
			failed++
		case c.notified:
			ready++
			notified++
		default:
			ready++
		}
	}
	return ready, notified, failed
}

// DiscardBlobs counts uploads and drops the bytes.
type DiscardBlobs struct{ puts, bytes atomic.Int64 }

func (b *DiscardBlobs) Put(_ context.Context, _ string, data []byte, _ string) error {
	b.puts.Add(1)
	b.bytes.Add(int64(len(data)))
	return nil
}

// CountingEvents counts published events and drops them.
type CountingEvents struct{ n atomic.Int64 }

func (e *CountingEvents) Publish(context.Context, domain.ReadyEvent) error {
	e.n.Add(1)
	return nil
}
