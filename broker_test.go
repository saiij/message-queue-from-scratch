package main

import (
	"testing"
	"time"
)

func newTestBroker(timeout time.Duration) *Broker {
	q := NewQueue(10)
	b := NewBroker(q, timeout)
	go b.watchInFlight()
	return b
}

func publishTestMessage(q *Queue, id string) {
	q.Publish(NewMessage(id, []byte("payload-"+id)))
}

func TestBroker_ConsumeDeliversMessage(t *testing.T) {
	b := newTestBroker(1 * time.Second)

	publishTestMessage(b.queue, "1")

	d := b.Consume()
	if d == nil {
		t.Fatal("expected delivery, got nil")
	}

	if d.Message.ID != "1" {
		t.Fatalf("expected message ID 1, got %s", d.Message.ID)
	}
}

func TestBroker_AckRemovesInFlight(t *testing.T) {
	b := newTestBroker(200 * time.Millisecond)

	publishTestMessage(b.queue, "1")

	d := b.Consume()
	d.Ack()

	select {
	case <-time.After(150 * time.Millisecond):
		// OK: no volvio
	case msg := <-b.queue.items:
		t.Fatalf("message %s should not have been requeued", msg.ID)
	}
}

func TestBroker_NackRequeuesMessage(t *testing.T) {
	b := newTestBroker(1 * time.Second)

	publishTestMessage(b.queue, "1")

	d := b.Consume()
	d.Nack()

	d2 := b.Consume()
	if d2.Message.ID != "1" {
		t.Fatalf("expected requeued message, got %s", d2.Message.ID)
	}
}

func TestBroker_TimeoutRequeuesMessage(t *testing.T) {
	b := newTestBroker(50 * time.Millisecond)

	publishTestMessage(b.queue, "1")

	d := b.Consume()
	_ = d // no ACK

	time.Sleep(80 * time.Millisecond)

	// deberia hacer requeue ya que el limite es 50ms y usamos un sleep de 80 ms
	d2 := b.Consume()
	if d2.Message.ID != "1" {
		t.Fatalf("expected message to be requeued after timeout")
	}
}

func TestBroker_LateAckDoesNotDuplicate(t *testing.T) {
	b := newTestBroker(50 * time.Millisecond)

	publishTestMessage(b.queue, "1")

	d := b.Consume()
	time.Sleep(80 * time.Millisecond)

	// ya fue requeued por que ya pasaron los 50 ms
	d.Ack()

	d2 := b.Consume()
	if d2.Message.ID != "1" {
		t.Fatalf("expected message after timeout")
	}

	select {
	case msg := <-b.queue.items:
		t.Fatalf("duplicate message detected: %s", msg.ID)
	case <-time.After(50 * time.Millisecond):
	}
}
