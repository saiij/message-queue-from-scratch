package main

import (
	"sync"
	"time"
)

type Broker struct {
	queue      *Queue
	inFlight   map[string]*InFlight
	next       int
	mu         sync.RWMutex
	consumers  []*Consumer
	ackTimeout time.Duration
}

func NewBroker(q *Queue, ackTimeout time.Duration) *Broker {
	return &Broker{
		queue:      q,
		inFlight:   make(map[string]*InFlight),
		ackTimeout: ackTimeout,
		consumers:  []*Consumer{},
		next:       0,
	}
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

func (b *Broker) RegisterConsumer(id string) <-chan *Delivery {
	b.mu.Lock()
	defer b.mu.Unlock()
	consumer := NewConsumer(id)
	b.consumers = append(b.consumers, consumer)
	return consumer.deliverCh
}

func (b *Broker) dispatch() {
	for {
		msg := b.queue.Consume()

		b.mu.Lock()
		//	for len(b.consumers) == 0 {
		//		b.mu.Unlock()
		//		time.Sleep(10 * time.Millisecond)
		//		b.mu.Lock()
		//	}

		consumer := b.nextConsumer()

		deadline := time.Now().Add(b.ackTimeout)
		b.inFlight[msg.ID] = &InFlight{
			msg:      msg,
			deadline: deadline,
		}
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

		consumer.deliverCh <- delivery
	}
}

func (b *Broker) nextConsumer() *Consumer {
	consumer := b.consumers[b.next]
	b.next = (b.next + 1) % len(b.consumers)
	return consumer
}
