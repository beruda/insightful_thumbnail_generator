package testkit

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sync"

	"github.com/vukkremic/thumbnailer/internal/domain"
	"github.com/vukkremic/thumbnailer/internal/scanner"
	"github.com/vukkremic/thumbnailer/internal/storage"
)

// Faults makes chosen methods of a wrapped fake fail on demand:
//
//	h.Repo.FailNext("Upsert", 2)          // the next two upserts fail
//	h.Repo.FailAlways("Get")
//	h.Blobs.FailRandomly("Put", 0.05, rng) // each call fails with 5% probability
type Faults struct {
	mu     sync.Mutex
	next   map[string]int
	always map[string]bool
	random map[string]randomFault
	calls  map[string]int
}

type randomFault struct {
	p   float64
	rng func() float64
}

func (f *Faults) FailNext(method string, times int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.init()
	f.next[method] = times
}

func (f *Faults) FailAlways(method string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.init()
	f.always[method] = true
}

func (f *Faults) FailRandomly(method string, probability float64, rng func() float64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.init()
	f.random[method] = randomFault{probability, rng}
}

func (f *Faults) Heal() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.next, f.always, f.random = nil, nil, nil
	f.init()
}

func (f *Faults) Calls(method string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls[method]
}

func (f *Faults) check(method string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.init()
	f.calls[method]++
	if f.next[method] > 0 {
		f.next[method]--
		return fmt.Errorf("injected %s failure", method)
	}
	if f.always[method] {
		return fmt.Errorf("injected %s failure", method)
	}
	if r, ok := f.random[method]; ok && r.rng() < r.p {
		return fmt.Errorf("injected random %s failure", method)
	}
	return nil
}

func (f *Faults) init() {
	if f.calls == nil {
		f.calls = map[string]int{}
	}
	if f.next == nil {
		f.next = map[string]int{}
	}
	if f.always == nil {
		f.always = map[string]bool{}
	}
	if f.random == nil {
		f.random = map[string]randomFault{}
	}
}

// Seeded returns a goroutine-safe, reproducible random source in [0, 1).
func Seeded(seed uint64) func() float64 {
	var mu sync.Mutex
	r := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
	return func() float64 {
		mu.Lock()
		defer mu.Unlock()
		return r.Float64()
	}
}

type Blobs struct {
	*storage.MemoryBlobStore
	Faults
}

func (b *Blobs) Put(ctx context.Context, key string, data []byte, contentType string) error {
	if err := b.check("Put"); err != nil {
		return err
	}
	return b.MemoryBlobStore.Put(ctx, key, data, contentType)
}

type Repo struct {
	*storage.MemoryRepository
	Faults
	// BeforeUpsert, if set, runs before each upsert; returning an error fails the upsert.
	BeforeUpsert func(domain.ThumbnailRecord) error
}

func (r *Repo) Get(ctx context.Context, videoPath string) (domain.ThumbnailRecord, bool, error) {
	if err := r.check("Get"); err != nil {
		return domain.ThumbnailRecord{}, false, err
	}
	return r.MemoryRepository.Get(ctx, videoPath)
}

func (r *Repo) Upsert(ctx context.Context, rec domain.ThumbnailRecord) error {
	if err := r.check("Upsert"); err != nil {
		return err
	}
	if r.BeforeUpsert != nil {
		if err := r.BeforeUpsert(rec); err != nil {
			return err
		}
	}
	return r.MemoryRepository.Upsert(ctx, rec)
}

type Events struct {
	storage.MemoryEvents
	Faults
}

func (e *Events) Publish(ctx context.Context, ev domain.ReadyEvent) error {
	if err := e.check("Publish"); err != nil {
		return err
	}
	return e.MemoryEvents.Publish(ctx, ev)
}

type Checkpoints struct {
	scanner.MemoryCheckpoints
	Faults
}

func (c *Checkpoints) Load(ctx context.Context) (*scanner.Checkpoint, error) {
	if err := c.check("Load"); err != nil {
		return nil, err
	}
	return c.MemoryCheckpoints.Load(ctx)
}

func (c *Checkpoints) Save(ctx context.Context, cp scanner.Checkpoint) error {
	if err := c.check("Save"); err != nil {
		return err
	}
	return c.MemoryCheckpoints.Save(ctx, cp)
}

func (c *Checkpoints) Clear(ctx context.Context) error {
	if err := c.check("Clear"); err != nil {
		return err
	}
	return c.MemoryCheckpoints.Clear(ctx)
}
