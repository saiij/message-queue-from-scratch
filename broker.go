package main

import (
	"sync"
	"time"
)

type Broker struct {
	queue      *Queue
	inFlight   map[string]*InFlight
	mu         sync.RWMutex
	ackTimeout time.Time
}

func NewBroker(q *Queue, ackTimeout time.Time) *Broker {
	return &Broker{
		queue:      q,
		inFlight:   make(map[string]*InFlight),
		ackTimeout: ackTimeout,
	}
}

func (b *Broker) Consume() *Delivery {
	msg := b.queue.Consume()
	inFlightMsg := NewInFlight(msg, b.ackTimeout)
	b.inFlight[msg.ID] = inFlightMsg

	delivery := NewDelivery(msg, b.ack(msg.ID), b.nack(msg.ID))

	return delivery
}

// ack() -> esto es para proceso exitoso
func (b *Broker) ack(id string) {
	delete(b.inFlight, id)
}

// nack() -> proceso no exitoso , requeue y quitar del map
func (b *Broker) nack(id string) {
	message, ok := b.inFlight[id]
	if !ok {
		return
	}

	b.queue.Publish(message.msg)
	delete(b.inFlight, id)
}
