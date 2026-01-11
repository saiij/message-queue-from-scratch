package main

type Message struct {
	ID      string
	Payload []byte
}

func NewMessage(id string, payload []byte) *Message {
	return &Message{
		ID:      id,
		Payload: payload,
	}
}
