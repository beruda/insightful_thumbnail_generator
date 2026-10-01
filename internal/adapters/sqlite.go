package adapters

import (
	"context"
	"database/sql"
	"errors"
	"slices"
	"strings"
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

const columns = `video_path, source_version, status, blob_key, byte_length, content_type,
	failure_kind, failure_reason, attempts, notified, updated_at`

// getManyChunk keeps IN (...) lists well under SQLite's bound-parameter limit.
const getManyChunk = 500

func (r *SQLiteRepository) Get(ctx context.Context, videoPath string) (domain.ThumbnailRecord, bool, error) {
	rec, err := scanRecord(r.db.QueryRowContext(ctx, "SELECT "+columns+" FROM thumbnails WHERE video_path = ?", videoPath))
	if errors.Is(err, sql.ErrNoRows) {
		return domain.ThumbnailRecord{}, false, nil
	}
	if err != nil {
		return domain.ThumbnailRecord{}, false, err
	}
	return rec, true, nil
}

func (r *SQLiteRepository) GetMany(ctx context.Context, videoPaths []string) (map[string]domain.ThumbnailRecord, error) {
	out := make(map[string]domain.ThumbnailRecord, len(videoPaths))
	for chunk := range slices.Chunk(videoPaths, getManyChunk) {
		args := make([]any, len(chunk))
		for i, p := range chunk {
			args[i] = p
		}
		placeholders := strings.TrimSuffix(strings.Repeat("?,", len(chunk)), ",")
		rows, err := r.db.QueryContext(ctx, "SELECT "+columns+" FROM thumbnails WHERE video_path IN ("+placeholders+")", args...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			rec, err := scanRecord(rows)
			if err != nil {
				_ = rows.Close()
				return nil, err
			}
			out[rec.VideoPath] = rec
		}
		if err := rows.Close(); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func scanRecord(row interface{ Scan(...any) error }) (domain.ThumbnailRecord, error) {
	var rec domain.ThumbnailRecord
	var status string
	var notified int
	var updated int64
	err := row.Scan(&rec.VideoPath, &rec.SourceVersion, &status, &rec.BlobKey, &rec.ByteLength, &rec.ContentType,
		&rec.FailureKind, &rec.FailureReason, &rec.Attempts, &notified, &updated)
	rec.Status = domain.Status(status)
	rec.Notified = notified == 1
	rec.UpdatedAt = time.UnixMilli(updated)
	return rec, err
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
