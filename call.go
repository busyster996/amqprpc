package amqprpc

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/segmentio/ksuid"
)

func (r *rpc) CallWithTimeout(timeout time.Duration, node, method string, data []byte) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return r.Call(ctx, node, method, data)
}

func (r *rpc) Call(ctx context.Context, node, method string, data []byte) ([]byte, error) {
	r.wg.Add(1)
	defer r.wg.Done()

	if node == "" {
		node = r.node
	}
	method = strings.ToLower(method)
	// 判断本地有处理方法
	if node == r.node {
		return r.callLocal(method, data)
	}
	return r.callRemote(ctx, node, method, data)
}

func (r *rpc) callLocal(method string, data []byte) ([]byte, error) {
	handler, ok := r.reqHandler.Load(method)
	if !ok {
		return nil, ErrHandlerNotFound
	}
	return handler.(HandlerFunc)(data)
}

func (r *rpc) callRemote(ctx context.Context, node, method string, data []byte) ([]byte, error) {
	var (
		ready      int32
		id         = ksuid.New().String()
		resCh      = make(chan response, 15)
		sendErrCh  = make(chan error)
		handlerKey = strings.ToLower(method + id)
	)

	r.rspHandler.Store(handlerKey, resCh)
	defer func() {
		r.rspHandler.Delete(handlerKey)
		close(resCh)
	}()

	// 启动握手
	go r.startHandshake(node, method, id, sendErrCh)

	// 阻塞式等待，哪个channel有消息就处理哪个
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case res := <-resCh:
			if bytes.Equal(res.Data, []byte(handshakeAckData)) {
				if !atomic.CompareAndSwapInt32(&ready, 0, 1) {
					continue
				}
				// 发送实际请求
				go func() {
					err := r.send(res.Base.RoutingKey, &request{
						Base: base{
							ID:         id,
							Method:     method,
							Time:       time.Now(),
							RoutingKey: r.node + routingKeySuffixRsp,
						},
						Action: ActionCall,
						Data:   data,
					})
					if err != nil {
						sendErrCh <- err
					}
				}()
				continue
			}
			if res.Error != "" {
				// 响应错误
				return nil, errors.New(res.Error)
			}
			return res.Data, nil
		case err := <-sendErrCh:
			return nil, fmt.Errorf("%w: %v", ErrSendFailed, err)
		case <-time.After(handshakeTimeout):
			if atomic.LoadInt32(&ready) == 0 {
				go r.startHandshake(node, method, id, sendErrCh)
			}
		}
	}
}
