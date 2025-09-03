package amqprpc

import "errors"

var (
	ErrEmptyExchange      = errors.New("exchange cannot be empty")
	ErrInvalidConcurrency = errors.New("concurrency must be between 1 and 1024")
	ErrHandlerExists      = errors.New("handler already exists")
	ErrHandlerNotFound    = errors.New("handler does not exist")
	ErrSendFailed         = errors.New("failed to send message")
)
