package scanner

import (
	"context"
	"sync"
)

type Checkpoint struct {
	Root     string   `json:"root"`
	Position Position `json:"position"`
}

// CheckpointStore persists scan progress so a restarted scanner resumes instead of starting over.
type CheckpointStore interface {
	// Load returns the saved checkpoint, or nil if there is none.
	Load(ctx context.Context) (*Checkpoint, error)
	Save(ctx context.Context, cp Checkpoint) error
	Clear(ctx context.Context) error
}

type MemoryCheckpoints struct {
	mu    sync.Mutex
	value *Checkpoint
	Saves int
}

func (m *MemoryCheckpoints) Load(context.Context) (*Checkpoint, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.value == nil {
		return nil, nil
	}
	cp := clone(*m.value)
	return &cp, nil
}

func (m *MemoryCheckpoints) Save(_ context.Context, cp Checkpoint) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	c := clone(cp)
	m.value = &c
	m.Saves++
	return nil
}

func (m *MemoryCheckpoints) Clear(context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.value = nil
	return nil
}

func clone(cp Checkpoint) Checkpoint {
	out := Checkpoint{Root: cp.Root, Position: Position{Pending: append([]string(nil), cp.Position.Pending...)}}
	if cp.Position.Current != nil {
		c := *cp.Position.Current
		out.Position.Current = &c
	}
	return out
}
