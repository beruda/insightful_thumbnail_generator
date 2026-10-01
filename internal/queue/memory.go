package queue

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

var ErrClosed = errors.New("queue is closed")

type DeadLetter[T any] struct {
	Message       T
	Reason        string
	DeliveryCount int
}

type Stats struct {
	Ready, Delayed, InFlight, DeadLettered int
}

type envelope[T any] struct {
	msg   T
	count int
}

// Memory is an in-process Queue, used for role=all and in tests. It behaves like a work-queue
// broker but lives in memory, so its contents are lost on restart. That's acceptable here because
// the scanner rebuilds the backlog on its next pass.
type Memory[T any] struct {
	mu       sync.Mutex
	capacity int
	ready    []*envelope[T]
	delayed  map[*envelope[T]]struct{}
	inFlight int
	closed   bool
	dead     []DeadLetter[T]
	// changed is closed (and replaced) whenever state changes. Waiters grab it under the lock
	// and then wait outside it, so a change can never slip in between "check" and "wait".
	changed chan struct{}
}

// NewMemory returns a queue that holds at most capacity ready messages before Publish blocks.
func NewMemory[T any](capacity int) *Memory[T] {
	if capacity < 1 {
		panic("queue capacity must be >= 1")
	}
	return &Memory[T]{capacity: capacity, delayed: map[*envelope[T]]struct{}{}, changed: make(chan struct{})}
}

func (q *Memory[T]) Publish(ctx context.Context, msg T) error {
	q.mu.Lock()
	for len(q.ready) >= q.capacity && !q.closed {
		changed := q.changed
		q.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		}
		q.mu.Lock()
	}
	defer q.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if q.closed {
		return ErrClosed
	}
	q.ready = append(q.ready, &envelope[T]{msg: msg})
	q.notifyLocked()
	return nil
}

func (q *Memory[T]) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = true
	q.notifyLocked()
}

func (q *Memory[T]) Consume(ctx context.Context, concurrency int, handle func(Delivery[T]) error) error {
	if concurrency < 1 {
		return fmt.Errorf("concurrency must be >= 1, got %d", concurrency)
	}
	var wg sync.WaitGroup
	running := 0 // guarded by q.mu

	for {
		q.mu.Lock()
		for ctx.Err() == nil && running < concurrency && len(q.ready) > 0 {
			env := q.ready[0]
			q.ready = q.ready[1:]
			env.count++
			q.inFlight++
			running++
			q.notifyLocked() // there's room now, so a blocked publisher can continue
			wg.Add(1)
			go func() {
				defer wg.Done()
				q.dispatch(env, handle)
				q.mu.Lock()
				q.inFlight--
				running--
				q.notifyLocked() // after freeing the slot, so the consumer sees it
				q.mu.Unlock()
			}()
		}
		if ctx.Err() != nil || q.drainedLocked() {
			q.mu.Unlock()
			break
		}
		changed := q.changed
		q.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
		}
	}
	wg.Wait()
	return nil
}

func (q *Memory[T]) dispatch(env *envelope[T], handle func(Delivery[T]) error) {
	d := &delivery[T]{q: q, env: env}
	defer func() {
		// A handler that panics, errors, or forgets to settle is a bug, not a job outcome.
		// Like a broker would, redeliver (Nack is a no-op if the delivery was already settled).
		_ = recover()
		_ = d.Nack(0)
	}()
	_ = handle(d)
}

func (q *Memory[T]) drainedLocked() bool {
	return q.closed && len(q.ready) == 0 && len(q.delayed) == 0 && q.inFlight == 0
}

func (q *Memory[T]) notifyLocked() {
	close(q.changed)
	q.changed = make(chan struct{})
}

func (q *Memory[T]) Stats() Stats {
	q.mu.Lock()
	defer q.mu.Unlock()
	return Stats{Ready: len(q.ready), Delayed: len(q.delayed), InFlight: q.inFlight, DeadLettered: len(q.dead)}
}

func (q *Memory[T]) DeadLetters() []DeadLetter[T] {
	q.mu.Lock()
	defer q.mu.Unlock()
	return append([]DeadLetter[T](nil), q.dead...)
}

// Pending returns messages that are ready or waiting for redelivery (not in flight).
func (q *Memory[T]) Pending() []T {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make([]T, 0, len(q.ready)+len(q.delayed))
	for _, env := range q.ready {
		out = append(out, env.msg)
	}
	for env := range q.delayed {
		out = append(out, env.msg)
	}
	return out
}

type delivery[T any] struct {
	q       *Memory[T]
	env     *envelope[T]
	settled bool // guarded by q.mu
}

func (d *delivery[T]) Message() T         { return d.env.msg }
func (d *delivery[T]) DeliveryCount() int { return d.env.count }

func (d *delivery[T]) Ack() error {
	d.settle(func() {})
	return nil
}

func (d *delivery[T]) Nack(delay time.Duration) error {
	q := d.q
	d.settle(func() {
		if delay <= 0 {
			q.ready = append(q.ready, d.env)
			return
		}
		q.delayed[d.env] = struct{}{}
		time.AfterFunc(delay, func() {
			q.mu.Lock()
			defer q.mu.Unlock()
			delete(q.delayed, d.env)
			q.ready = append(q.ready, d.env)
			q.notifyLocked()
		})
	})
	return nil
}

func (d *delivery[T]) DeadLetter(reason string) error {
	d.settle(func() {
		d.q.dead = append(d.q.dead, DeadLetter[T]{Message: d.env.msg, Reason: reason, DeliveryCount: d.env.count})
	})
	return nil
}

// settle runs action once, under the lock; later settlements of the same delivery are ignored.
func (d *delivery[T]) settle(action func()) {
	d.q.mu.Lock()
	defer d.q.mu.Unlock()
	if d.settled {
		return
	}
	d.settled = true
	action()
	d.q.notifyLocked()
}
