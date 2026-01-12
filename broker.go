package main

import (
	"context"
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
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
}

func NewBroker(q *Queue, ackTimeout time.Duration) *Broker {
	ctx, cancel := context.WithCancel(context.Background())
	b := &Broker{
		queue:      q,
		inFlight:   make(map[string]*InFlight),
		ackTimeout: ackTimeout,
		consumers:  []*Consumer{},
		next:       0,
		ctx:        ctx,
		cancel:     cancel,
	}

	// iniciar goroutines
	b.wg.Add(2)
	go b.watchInFlight()
	go b.dispatch()

	return b
}

func (b *Broker) watchInFlight() {
	defer b.wg.Done()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-b.ctx.Done():
			return
		case <-ticker.C:
			now := time.Now()
			b.mu.Lock()
			for id, inflight := range b.inFlight {
				if now.After(inflight.deadline) {
					delete(b.inFlight, id)
					// reintetnar publicar ( ignora error si queue esta cerrada)
					_ = b.queue.Publish(inflight.msg)
				}
			}
			b.mu.Unlock()
		}
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
	defer b.wg.Done()

	for {
		select {
		case <-b.ctx.Done():
			return
		default:
			msg, ok := b.queue.Consume()
			if !ok {
				// Queue cerrada
				return
			}

			b.mu.Lock()
			// Esperar si no hay consumers
			if len(b.consumers) == 0 {
				b.mu.Unlock()
				// Re-publicar mensaje
				_ = b.queue.Publish(msg)
				time.Sleep(100 * time.Millisecond)
				continue
			}

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
					_ = b.queue.Publish(inflight.msg)
				},
			)

			// Envio no bloqueante
			select {
			case consumer.deliverCh <- delivery:
			case <-b.ctx.Done():
				return
			}
		}
	}
}

func (b *Broker) nextConsumer() *Consumer {
	consumer := b.consumers[b.next]
	b.next = (b.next + 1) % len(b.consumers)
	return consumer
}

func (b *Broker) Shutdwon() {
	b.cancel()
	b.wg.Wait()

	// cerrar channels
	b.mu.Lock()
	for _, c := range b.consumers {
		close(c.deliverCh)
	}
	b.mu.Unlock()
}
