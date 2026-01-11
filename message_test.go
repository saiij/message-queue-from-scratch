package main

import (
	"bytes"
	"testing"
	"time"
)

func TestQueue_PublishAndSubscribe(t *testing.T) {
	q := NewQueue(10)
	testPayload := []byte("first message")
	msg := NewMessage("test-id-1", testPayload)

	// 1. publicar message
	err := q.Publish(msg)
	if err != nil {
		t.Fatalf("Publish() returned an unexpected error: %v", err)
	}

	// 2. consumir message
	receivedMessage := q.Consume()
	if receivedMessage.ID != msg.ID {
		t.Errorf("expected message ID %s, but got %s", msg.ID, receivedMessage.ID)
	}
	if !bytes.Equal(receivedMessage.Payload, msg.Payload) {
		t.Errorf("expected message payload %s, but got %s", string(msg.Payload), string(receivedMessage.Payload))
	}
}

func TestQueue_ConsumeBlocksWhenEmpty(t *testing.T) {
	q := NewQueue(10)

	consumed := make(chan struct{})

	go func() {
		q.Consume()
		close(consumed)
	}()
	select {
	case <-consumed:
		t.Fatal("Consume () did not block when the queue was empty")
	case <-time.After(100 * time.Millisecond):
	}
}

func TestQueue_FIFOOrder(t *testing.T) {
	q := NewQueue(10)

	q.Publish(NewMessage("1", []byte("one")))
	q.Publish(NewMessage("2", []byte("two")))
	q.Publish(NewMessage("3", []byte("three")))

	if q.Consume().ID != "1" {
		t.Fatal("expected FIFO order")
	}
	if q.Consume().ID != "2" {
		t.Fatal("expected FIFO order")
	}
	if q.Consume().ID != "3" {
		t.Fatal("expected FIFO order")
	}
}

func TestQueue_PublishBlocksWhenFull(t *testing.T) {
	q := NewQueue(1)

	q.Publish(NewMessage("1", []byte("one")))

	done := make(chan struct{})
	go func() {
		q.Publish(NewMessage("2", []byte("two")))
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("Publish should block when queue is full")
	case <-time.After(100 * time.Millisecond):
		// OK
	}
}
