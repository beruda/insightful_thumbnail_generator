package domain

import (
	"context"
	"time"
)

// BlobStore holds thumbnail bytes: object storage (S3/GCS behind a CDN) in production.
// Keys are immutable per source version, so re-uploads are idempotent and CDN caches never
// need invalidating.
type BlobStore interface {
	// Put atomically stores data under key, overwriting any existing object.
	Put(ctx context.Context, key string, data []byte, contentType string) error
}

type Status string

const (
	StatusReady  Status = "ready"
	StatusFailed Status = "failed"
)

// ThumbnailRecord is the metadata row for one video (Postgres/DynamoDB in production).
type ThumbnailRecord struct {
	VideoPath string
	// SourceVersion the record describes; a different version means the record is stale.
	SourceVersion string
	Status        Status
	BlobKey       string
	ByteLength    int
	ContentType   string
	FailureKind   string // set when Status is failed
	FailureReason string
	Attempts      int
	// Notified records whether the "thumbnail ready" event went out, so it can be re-sent if not.
	Notified  bool
	UpdatedAt time.Time
}

type Repository interface {
	// Get returns the record for videoPath, and false if there is none.
	Get(ctx context.Context, videoPath string) (ThumbnailRecord, bool, error)
	Upsert(ctx context.Context, record ThumbnailRecord) error
}

// IsSettled reports whether record already settles sourceVersion: the thumbnail exists, or the
// video failed permanently. A failed video is only retried once its source changes.
func IsSettled(record ThumbnailRecord, found bool, sourceVersion string) bool {
	return found && record.SourceVersion == sourceVersion
}

// NeedsWork reports whether the scanner should publish a job: there's no settled record, or the
// thumbnail exists but its "ready" event was never delivered. The second case makes each scan
// pass double as a sweep that re-sends lost events.
func NeedsWork(record ThumbnailRecord, found bool, sourceVersion string) bool {
	if !IsSettled(record, found, sourceVersion) {
		return true
	}
	return record.Status == StatusReady && !record.Notified
}

// ReadyEvent is the downstream "done" signal, e.g. for a UI or search indexer.
type ReadyEvent struct {
	Type          string `json:"type"` // always "thumbnail.ready"
	VideoPath     string `json:"videoPath"`
	SourceVersion string `json:"sourceVersion"`
	BlobKey       string `json:"blobKey"`
	ContentType   string `json:"contentType"`
}

type EventPublisher interface {
	Publish(ctx context.Context, event ReadyEvent) error
}
