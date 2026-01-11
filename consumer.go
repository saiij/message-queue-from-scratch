package main

import "time"

type Consumer struct {
	id        string
	deliverCh chan *Delivery
}

/*
* registrar consumer :
* 1 channel por consumer
* dispatcher :
* espera mensaje -> elige consumer -> dispatch
*
* */

type Delivery struct {
	Message *Message
	Ack     func()
	Nack    func()
}

type InFlight struct {
	msg      *Message
	deadline time.Time
}

func NewConsumer(id string) *Consumer {
	return &Consumer{
		id:        id,
		deliverCh: make(chan *Delivery, 10),
	}
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
