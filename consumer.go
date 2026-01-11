package main

import "time"

type Delivery struct {
	Message *Message
	Ack     func()
	Nack    func()
}

type InFlight struct {
	msg      *Message
	deadline time.Time
}

func NewDelivery(m *Message, ackFn, nackFn func()) *Delivery {
	return &Delivery{
		Message: m,
		Ack:     ackFn,
		Nack:    nackFn,
	}
}

func NewInFlight(m *Message, dl time.Time) *InFlight {
	return &InFlight{
		msg:      m,
		deadline: dl,
	}
}
