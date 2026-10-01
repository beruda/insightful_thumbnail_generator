package queue_test

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/vukkremic/thumbnailer/internal/queue"
	tk "github.com/vukkremic/thumbnailer/internal/testkit"
)

func filled(t *testing.T, capacity int, msgs ...string) *queue.Memory[string] {
	q := queue.NewMemory[string](capacity)
	for _, m := range msgs {
		tk.Equal(t, q.Publish(context.Background(), m), nil, "publish")
	}
	return q
}

func messages(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("m%02d", i)
	}
	return out
}

func TestMemoryQueue(t *testing.T) {
	ctx := context.Background()

	t.Run("delivers every message once when handlers ack", func(t *testing.T) {
		q := filled(t, 100, "a", "b", "c")
		q.Close()
		var mu sync.Mutex
		var seen []string
		_ = q.Consume(ctx, 2, func(d queue.Delivery[string]) error {
			mu.Lock()
			seen = append(seen, d.Message())
			mu.Unlock()
			return d.Ack()
		})
		slices.Sort(seen)
		tk.Equal(t, seen, []string{"a", "b", "c"}, "seen")
		tk.Equal(t, q.Stats(), queue.Stats{}, "stats")
	})

	t.Run("regression: no lost wake-up when a slot frees up", func(t *testing.T) {
		// A lost wake-up only bites under one specific interleaving, so: many rounds, handlers
		// that yield to the scheduler, and a synctest bubble, which turns a stuck consumer into an
		// immediate deadlock failure instead of a hang.
		for range 200 {
			synctest.Test(t, func(t *testing.T) {
				q := filled(t, 100, messages(20)...)
				q.Close()
				var n atomic.Int32
				_ = q.Consume(context.Background(), 1, func(d queue.Delivery[string]) error {
					runtime.Gosched()
					n.Add(1)
					return d.Ack()
				})
				tk.Equal(t, n.Load(), int32(20), "handled")
			})
		}
	})

	t.Run("never runs more than `concurrency` handlers at once", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			q := filled(t, 100, messages(20)...)
			q.Close()
			var running, peak atomic.Int32
			_ = q.Consume(context.Background(), 3, func(d queue.Delivery[string]) error {
				now := running.Add(1)
				for {
					old := peak.Load()
					if now <= old || peak.CompareAndSwap(old, now) {
						break
					}
				}
				time.Sleep(10 * time.Millisecond)
				running.Add(-1)
				return d.Ack()
			})
			tk.Equal(t, peak.Load(), int32(3), "peak")
		})
	})

	t.Run("blocks publishers while full and releases them as messages are consumed", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			q := filled(t, 2, "a", "b")
			published := make(chan error)
			go func() { published <- q.Publish(context.Background(), "c") }()
			synctest.Wait()
			select {
			case <-published:
				t.Fatal("publish should block while the queue is full")
			default:
			}

			ctx, cancel := context.WithCancel(context.Background())
			go func() { _ = q.Consume(ctx, 1, func(d queue.Delivery[string]) error { return d.Ack() }) }()
			tk.Equal(t, <-published, nil, "publish error")
			cancel()
		})
	})

	t.Run("a blocked publish fails when its context is cancelled", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			q := filled(t, 1, "a")
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error)
			go func() { done <- q.Publish(ctx, "b") }()
			synctest.Wait()
			cancel()
			tk.True(t, errors.Is(<-done, context.Canceled), "want context.Canceled")
		})
	})

	t.Run("redelivers a nacked message after its delay, counting deliveries", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			q := filled(t, 10, "job")
			q.Close()
			start := time.Now()
			type delivery struct {
				count int
				at    time.Duration
			}
			var got []delivery
			_ = q.Consume(context.Background(), 1, func(d queue.Delivery[string]) error {
				got = append(got, delivery{d.DeliveryCount(), time.Since(start)})
				if d.DeliveryCount() < 3 {
					return d.Nack(time.Second)
				}
				return d.Ack()
			})
			tk.Equal(t, got, []delivery{{1, 0}, {2, time.Second}, {3, 2 * time.Second}}, "deliveries")
		})
	})

	t.Run("dead-letters without redelivering", func(t *testing.T) {
		q := filled(t, 10, "poison")
		q.Close()
		calls := 0
		_ = q.Consume(ctx, 1, func(d queue.Delivery[string]) error { calls++; return d.DeadLetter("bad input") })
		tk.Equal(t, calls, 1, "deliveries")
		tk.Equal(t, q.DeadLetters(), []queue.DeadLetter[string]{{Message: "poison", Reason: "bad input", DeliveryCount: 1}}, "dead letters")
	})

	for name, handler := range map[string]func(attempt int, d queue.Delivery[string]) error{
		"errors":            func(int, queue.Delivery[string]) error { return errors.New("bug") },
		"panics":            func(int, queue.Delivery[string]) error { panic("bug") },
		"returns unsettled": func(int, queue.Delivery[string]) error { return nil },
	} {
		t.Run("a handler that "+name+" is treated as a nack (redelivered, not lost)", func(t *testing.T) {
			q := filled(t, 10, "job")
			q.Close()
			attempts := 0
			_ = q.Consume(ctx, 1, func(d queue.Delivery[string]) error {
				attempts++
				if attempts == 1 {
					return handler(attempts, d)
				}
				return d.Ack()
			})
			tk.Equal(t, attempts, 2, "attempts")
		})
	}

	t.Run("a second settlement of the same delivery is ignored", func(t *testing.T) {
		q := filled(t, 10, "job")
		q.Close()
		_ = q.Consume(ctx, 1, func(d queue.Delivery[string]) error {
			_ = d.Ack()
			_ = d.Nack(0)            // must not redeliver
			return d.DeadLetter("x") // must not dead-letter
		})
		tk.Equal(t, len(q.DeadLetters()), 0, "dead letters")
		tk.Equal(t, len(q.Pending()), 0, "pending")
	})

	t.Run("on cancel: stops taking work, waits for in-flight handlers, leaves the rest queued", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			q := filled(t, 10, "m0", "m1", "m2", "m3", "m4")
			ctx, cancel := context.WithCancel(context.Background())
			var mu sync.Mutex
			var handled []string
			done := make(chan error)
			go func() {
				done <- q.Consume(ctx, 2, func(d queue.Delivery[string]) error {
					time.Sleep(100 * time.Millisecond)
					mu.Lock()
					handled = append(handled, d.Message())
					mu.Unlock()
					return d.Ack()
				})
			}()
			synctest.Wait()
			cancel()
			<-done
			slices.Sort(handled)
			tk.Equal(t, handled, []string{"m0", "m1"}, "in-flight work completed")
			tk.Equal(t, q.Pending(), []string{"m2", "m3", "m4"}, "untouched work still queued")
		})
	})

	t.Run("an open queue keeps the consumer waiting for new messages", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			q := queue.NewMemory[string](10)
			ctx, cancel := context.WithCancel(context.Background())
			seen := make(chan string, 1)
			done := make(chan error)
			go func() {
				done <- q.Consume(ctx, 1, func(d queue.Delivery[string]) error { seen <- d.Message(); return d.Ack() })
			}()
			synctest.Wait()
			_ = q.Publish(context.Background(), "late")
			tk.Equal(t, <-seen, "late", "message")
			cancel()
			<-done
		})
	})

	t.Run("publishing to a closed queue fails", func(t *testing.T) {
		q := queue.NewMemory[string](1)
		q.Close()
		tk.True(t, errors.Is(q.Publish(ctx, "x"), queue.ErrClosed), "want ErrClosed")
	})
}
