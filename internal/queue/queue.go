// Package queue defines the work queue joining the scanner and the workers, plus an in-memory
// implementation.
//
// Semantics match a work-queue broker (Pub/Sub, SQS): at-least-once delivery, each message
// individually acked, nacked with an optional redelivery delay, or dead-lettered. Handlers must
// therefore be idempotent.
package queue

import (
	"context"
	"time"
)

type Queue[T any] interface {
	// Publish enqueues a message, waiting while the queue is full (backpressure).
	Publish(ctx context.Context, msg T) error
	// Consume runs handle with at most `concurrency` handlers at once. It returns once ctx is
	// cancelled and in-flight handlers have finished or, for a closed queue, once it's drained.
	// A handler that returns without settling its delivery (or returns an error) is nacked.
	Consume(ctx context.Context, concurrency int, handle func(Delivery[T]) error) error
	// Close signals that nothing more will be published, so Consume can return once drained.
	Close()
}

type Delivery[T any] interface {
	Message() T
	// DeliveryCount is 1 on first delivery and goes up by one on each redelivery.
	DeliveryCount() int
	Ack() error
	// Nack returns the message to the queue, to be redelivered after delay.
	Nack(delay time.Duration) error
	// DeadLetter moves the message to the dead-letter queue; it won't be redelivered.
	DeadLetter(reason string) error
}
