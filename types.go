package amqprpc

import "time"

type base struct {
	ID         string    `json:"id,omitempty"`
	Method     string    `json:"method,omitempty"`
	RoutingKey string    `json:"routing_key,omitempty"`
	Time       time.Time `json:"time,omitempty"`
}

type request struct {
	Base   base   `json:"base,omitempty"`
	Action string `json:"action,omitempty"`
	Data   []byte `json:"data,omitempty"`
}

type response struct {
	Base  base   `json:"base,omitempty"`
	Data  []byte `json:"data,omitempty"`
	Error string `json:"error,omitempty"`
}
