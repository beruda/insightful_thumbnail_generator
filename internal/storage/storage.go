// Package storage has the blob key scheme and in-memory implementations of the storage interfaces.
package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"maps"
	"sync"

	"github.com/vukkremic/thumbnailer/internal/domain"
)

var extensions = map[string]string{"image/jpeg": "jpg", "image/png": "png", "image/webp": "webp"}

// BlobKey returns the immutable object key for a video's thumbnail at one source version:
//
//	thumbnails/{shard}/{sha256(path)}/{sha256(version)}.jpg
//
// The key is:
//   - deterministic, so a retried or duplicated job overwrites the same object (idempotent);
//   - different whenever the video changes, so CDN caches can keep objects forever;
//   - sharded by a 2-character prefix that spreads keys across object-store partitions.
func BlobKey(videoPath, sourceVersion, contentType string) string {
	pathHash := hash(videoPath)[:32]
	versionHash := hash(sourceVersion)[:16]
	ext, ok := extensions[contentType]
	if !ok {
		ext = "bin"
	}
	return fmt.Sprintf("thumbnails/%s/%s/%s.%s", pathHash[:2], pathHash, versionHash, ext)
}

func hash(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

type Object struct {
	Data        []byte
	ContentType string
}

type MemoryBlobStore struct {
	mu      sync.Mutex
	objects map[string]Object
	puts    int
}

func NewMemoryBlobStore() *MemoryBlobStore {
	return &MemoryBlobStore{objects: map[string]Object{}}
}

func (s *MemoryBlobStore) Put(_ context.Context, key string, data []byte, contentType string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.puts++
	s.objects[key] = Object{Data: append([]byte(nil), data...), ContentType: contentType}
	return nil
}

func (s *MemoryBlobStore) Objects() map[string]Object {
	s.mu.Lock()
	defer s.mu.Unlock()
	return maps.Clone(s.objects)
}

func (s *MemoryBlobStore) Puts() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.puts
}

type MemoryRepository struct {
	mu      sync.Mutex
	records map[string]domain.ThumbnailRecord
}

func NewMemoryRepository() *MemoryRepository {
	return &MemoryRepository{records: map[string]domain.ThumbnailRecord{}}
}

func (r *MemoryRepository) Get(_ context.Context, videoPath string) (domain.ThumbnailRecord, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	rec, ok := r.records[videoPath]
	return rec, ok, nil
}

func (r *MemoryRepository) GetMany(_ context.Context, videoPaths []string) (map[string]domain.ThumbnailRecord, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[string]domain.ThumbnailRecord, len(videoPaths))
	for _, p := range videoPaths {
		if rec, ok := r.records[p]; ok {
			out[p] = rec
		}
	}
	return out, nil
}

func (r *MemoryRepository) Upsert(_ context.Context, rec domain.ThumbnailRecord) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.records[rec.VideoPath] = rec
	return nil
}

func (r *MemoryRepository) All() map[string]domain.ThumbnailRecord {
	r.mu.Lock()
	defer r.mu.Unlock()
	return maps.Clone(r.records)
}

type MemoryEvents struct {
	mu        sync.Mutex
	published []domain.ReadyEvent
}

func (e *MemoryEvents) Publish(_ context.Context, ev domain.ReadyEvent) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.published = append(e.published, ev)
	return nil
}

func (e *MemoryEvents) Published() []domain.ReadyEvent {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]domain.ReadyEvent(nil), e.published...)
}
