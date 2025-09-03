package amqprpc

import (
	"github.com/segmentio/ksuid"

	"github.com/busyster996/amqprpc/pkg/logx"
)

type Option func(*rpc) error

func WithNode(node string) Option {
	return func(r *rpc) error {
		if node == "" {
			node = ksuid.New().String()
			logx.Infoln("node:", node)
		}
		r.node = node
		return nil
	}
}

func WithExchange(exchange string) Option {
	return func(r *rpc) error {
		if exchange == "" {
			return ErrEmptyExchange
		}
		r.exchange = exchange
		return nil
	}
}

func WithConcurrency(concurrency int) Option {
	return func(r *rpc) error {
		if concurrency <= 0 {
			return ErrInvalidConcurrency
		}
		if concurrency > maxConcurrency {
			return ErrInvalidConcurrency
		}
		r.concurrency = concurrency
		return nil
	}
}
