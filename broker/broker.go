// Package broker is an in-memory pub-sub broker built on an append-only log.
package broker

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
)

var ErrClosed = errors.New("broker: closed")

type Message[T any] struct {
	Offset  uint64
	Payload T
}

type Broker[T any] struct {
	mu     sync.RWMutex
	log    []Message[T]
	notify chan struct{} // closed and replaced on every append
	closed bool
}

func New[T any]() *Broker[T] {
	return &Broker[T]{notify: make(chan struct{})}
}

func (b *Broker[T]) append(payload T) (uint64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return 0, ErrClosed
	}
	offset := uint64(len(b.log))
	b.log = append(b.log, Message[T]{Offset: offset, Payload: payload})

	close(b.notify)
	b.notify = make(chan struct{})
	return offset, nil
}

// read returns the messages from offset to the end of the log. With none left,
// it returns a channel that closes on the next append, or done once closed.
func (b *Broker[T]) read(offset uint64) (batch []Message[T], wait <-chan struct{}, done bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	n := uint64(len(b.log))
	if offset < n {
		// Entries below len are never rewritten, so this is safe to read unlocked.
		return b.log[offset:n:n], nil, false
	}
	if b.closed {
		return nil, nil, true
	}
	return nil, b.notify, false
}

// Close rejects new publishes. Subscribers drain the log, then stop.
func (b *Broker[T]) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return
	}
	b.closed = true
	close(b.notify)
}

type Publisher[T any] struct {
	broker *Broker[T]
}

func NewPublisher[T any](b *Broker[T]) *Publisher[T] {
	return &Publisher[T]{broker: b}
}

func (p *Publisher[T]) Publish(payload T) (uint64, error) {
	return p.broker.append(payload)
}

// Subscriber reads the log from offset 0 and calls handle for each message in
// order, from one goroutine.
type Subscriber[T any] struct {
	broker *Broker[T]
	handle func(Message[T])
	once   sync.Once
	offset atomic.Uint64
	done   chan struct{}
}

func NewSubscriber[T any](b *Broker[T], handle func(Message[T])) *Subscriber[T] {
	return &Subscriber[T]{broker: b, handle: handle, done: make(chan struct{})}
}

// Subscribe starts delivery. Calls after the first do nothing and return false.
func (s *Subscriber[T]) Subscribe(ctx context.Context) bool {
	started := false
	s.once.Do(func() {
		started = true
		go s.run(ctx)
	})
	return started
}

func (s *Subscriber[T]) Done() <-chan struct{} { return s.done }

// Offset is the next offset to read, equal to the number of messages handled.
func (s *Subscriber[T]) Offset() uint64 { return s.offset.Load() }

func (s *Subscriber[T]) run(ctx context.Context) {
	defer close(s.done)

	for {
		batch, wait, done := s.broker.read(s.offset.Load())
		if done {
			return
		}
		if len(batch) == 0 {
			select {
			case <-wait:
				continue
			case <-ctx.Done():
				return
			}
		}
		for _, msg := range batch {
			if ctx.Err() != nil {
				return
			}
			s.handle(msg)
			s.offset.Add(1)
		}
	}
}
