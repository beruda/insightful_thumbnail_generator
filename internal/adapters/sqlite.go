package adapters

import (
	"context"
	"database/sql"
	"errors"
	"time"

	_ "modernc.org/sqlite" // pure-Go driver, no cgo

	"github.com/vukkremic/thumbnailer/internal/domain"
)

// SQLiteRepository is the metadata store for local runs and the demo. The schema maps directly to
// Postgres; production would swap in that adapter, or DynamoDB keyed on video_path.
type SQLiteRepository struct{ db *sql.DB }

const schema = `
CREATE TABLE IF NOT EXISTS thumbnails (
  video_path      TEXT PRIMARY KEY,
  source_version  TEXT NOT NULL,
  status          TEXT NOT NULL CHECK (status IN ('ready', 'failed')),
  blob_key        TEXT NOT NULL DEFAULT '',
  byte_length     INTEGER NOT NULL DEFAULT 0,
  content_type    TEXT NOT NULL DEFAULT '',
  failure_kind    TEXT NOT NULL DEFAULT '',
  failure_reason  TEXT NOT NULL DEFAULT '',
  attempts        INTEGER NOT NULL,
  notified        INTEGER NOT NULL,
  updated_at      INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS thumbnails_status ON thumbnails (status, updated_at);`

func OpenSQLite(path string) (*SQLiteRepository, error) {
	db, err := sql.Open("sqlite", path+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)")
	if err != nil {
		return nil, err
	}
	if _, err := db.Exec(schema); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &SQLiteRepository{db: db}, nil
}

func (r *SQLiteRepository) Close() error { return r.db.Close() }

func (r *SQLiteRepository) Get(ctx context.Context, videoPath string) (domain.ThumbnailRecord, bool, error) {
	var rec domain.ThumbnailRecord
	var status string
	var notified int
	var updated int64
	err := r.db.QueryRowContext(ctx, `
		SELECT video_path, source_version, status, blob_key, byte_length, content_type,
		       failure_kind, failure_reason, attempts, notified, updated_at
		FROM thumbnails WHERE video_path = ?`, videoPath).
		Scan(&rec.VideoPath, &rec.SourceVersion, &status, &rec.BlobKey, &rec.ByteLength, &rec.ContentType,
			&rec.FailureKind, &rec.FailureReason, &rec.Attempts, &notified, &updated)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.ThumbnailRecord{}, false, nil
	}
	if err != nil {
		return domain.ThumbnailRecord{}, false, err
	}
	rec.Status = domain.Status(status)
	rec.Notified = notified == 1
	rec.UpdatedAt = time.UnixMilli(updated)
	return rec, true, nil
}

func (r *SQLiteRepository) Upsert(ctx context.Context, rec domain.ThumbnailRecord) error {
	notified := 0
	if rec.Notified {
		notified = 1
	}
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO thumbnails (video_path, source_version, status, blob_key, byte_length, content_type,
		                        failure_kind, failure_reason, attempts, notified, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (video_path) DO UPDATE SET
		  source_version = excluded.source_version, status = excluded.status, blob_key = excluded.blob_key,
		  byte_length = excluded.byte_length, content_type = excluded.content_type,
		  failure_kind = excluded.failure_kind, failure_reason = excluded.failure_reason,
		  attempts = excluded.attempts, notified = excluded.notified, updated_at = excluded.updated_at`,
		rec.VideoPath, rec.SourceVersion, string(rec.Status), rec.BlobKey, rec.ByteLength, rec.ContentType,
		rec.FailureKind, rec.FailureReason, rec.Attempts, notified, rec.UpdatedAt.UnixMilli())
	return err
}
