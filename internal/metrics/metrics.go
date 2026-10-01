// Package metrics provides in-process counters, reported by the health endpoint and the run
// summary. Production would export these to Prometheus/OpenTelemetry instead.
package metrics

import "sync"

type Counters struct {
	mu     sync.Mutex
	values map[string]int64
}

func New() *Counters {
	return &Counters{values: map[string]int64{}}
}

func (c *Counters) Inc(name string) { c.Add(name, 1) }

func (c *Counters) Add(name string, delta int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.values[name] += delta
}

func (c *Counters) Get(name string) int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.values[name]
}

func (c *Counters) Snapshot() map[string]int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[string]int64, len(c.values))
	for k, v := range c.values {
		out[k] = v
	}
	return out
}
