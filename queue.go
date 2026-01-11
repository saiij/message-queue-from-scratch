package main

import "sync"

const (
	// el size del buffer por defecto
	// un buffer perminte a los publishers enviar messages sin bloquearse
	// hasta que un subscribe este listo

	defaultQueueSize = 100
)

/*
Usar chan para items ya da FIFO por defecto.
*/
type Queue struct {
	mu    sync.RWMutex
	items chan *Message
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
	q.items <- m
	return nil
}

// consumir mensajes de la cola
func (q *Queue) Consume() *Message {
	return <-q.items // bloquea al consumer si esta vacia
}
