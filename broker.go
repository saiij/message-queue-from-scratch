package main

import (
	"sync"
	"time"
)

type Broker struct {
	queue      *Queue
	inFlight   map[string]*InFlight
	mu         sync.RWMutex
	ackTimeout time.Duration
}

func NewBroker(q *Queue, ackTimeout time.Duration) *Broker {
	return &Broker{
		queue:      q,
		inFlight:   make(map[string]*InFlight),
		ackTimeout: ackTimeout,
	}
}

func (b *Broker) Consume() *Delivery {
	msg := b.queue.Consume()

	deadline := time.Now().Add(b.ackTimeout)
	b.mu.Lock()
	inFlightMsg := NewInFlight(msg, deadline)
	b.inFlight[msg.ID] = inFlightMsg
	b.mu.Unlock()
	delivery := NewDelivery(
		msg,
		func() {
			b.mu.Lock()
			defer b.mu.Unlock()

			if _, ok := b.inFlight[msg.ID]; !ok {
				return
			}
			delete(b.inFlight, msg.ID)
		},
		func() {
			b.mu.Lock()
			defer b.mu.Unlock()

			inflight, ok := b.inFlight[msg.ID]
			if !ok {
				return
			}
			delete(b.inFlight, msg.ID)
			b.queue.Publish(inflight.msg)
		},
	)

	return delivery
}

func (b *Broker) watchInFlight() {
	ticker := time.NewTicker(10 * time.Millisecond)
	for range ticker.C {
		now := time.Now()
		b.mu.Lock()
		for id, inflight := range b.inFlight {
			if now.After(inflight.deadline) {
				delete(b.inFlight, id)
				b.queue.Publish(inflight.msg)
			}
		}
		b.mu.Unlock()
	}
}
