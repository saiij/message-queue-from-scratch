package main

import (
	"errors"
	"sync"
	"time"
)

const (
	// el size del buffer por defecto
	// un buffer perminte a los publishers enviar messages sin bloquearse
	// hasta que un subscribe este listo
	defaultQueueSize = 100
)

var (
	ErrQueueFull    = errors.New("queue is full")
	ErrQueueCloseed = errors.New("queue is closed")
)

/*
Usar chan para items ya da FIFO por defecto.
*/
type Queue struct {
	mu     sync.RWMutex
	closed bool
	items  chan *Message
}

func NewQueue(size int) *Queue {
	if size <= 0 {
		size = defaultQueueSize
	}
	return &Queue{
		items: make(chan *Message, size),
	}
}

// agregar message a la cola
func (q *Queue) Publish(m *Message) error {
	return q.PublishWithTimeout(m, 5*time.Second)
}

func (q *Queue) PublishWithTimeout(m *Message, timeout time.Duration) error {
	q.mu.RLock()
	if q.closed {
		q.mu.RUnlock()
		return ErrQueueCloseed
	}
	q.mu.RUnlock()
	select {
	case q.items <- m:
		return nil
	case <-time.After(timeout):
		return ErrQueueFull
	}
}

// consumir mensajes de la cola
func (q *Queue) Consume() (*Message, bool) {
	msg, ok := <-q.items // bloquea al consumer si esta vacia
	return msg, ok
}

func (q *Queue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if !q.closed {
		q.closed = true
		close(q.items)
	}
}
