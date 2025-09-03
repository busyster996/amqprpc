package amqprpc

import (
	"fmt"
	"strings"
	"time"

	"github.com/busyster996/amqprpc/pkg/logx"
	"github.com/busyster996/amqprpc/pkg/rabbitmq"
)

func (r *rpc) handler(delivery rabbitmq.Delivery) rabbitmq.Action {
	r.wg.Add(1)
	defer r.wg.Done()

	if strings.HasSuffix(strings.ToLower(delivery.RoutingKey), routingKeySuffixReq) {
		return r.handlerReq(delivery)
	}
	return r.handlerRsp(delivery)
}

func (r *rpc) handlerReq(delivery rabbitmq.Delivery) rabbitmq.Action {
	var pkg = new(request)
	if err := r.unpack(delivery.Body, pkg); err != nil {
		logx.Errorf("failed to unpack request: %v", err)
		return rabbitmq.NackDiscard
	}

	var res = &response{
		Base: base{
			ID:         pkg.Base.ID,
			Method:     pkg.Base.Method,
			Time:       time.Now(),
			RoutingKey: r.node + routingKeySuffixReq,
		},
	}
	method := strings.ToLower(pkg.Base.Method)
	switch strings.ToLower(pkg.Action) {
	case ActionShakehand:
		res.Data = []byte(handshakeAckData)
	default:
		// 时间超过5秒的请求，将丢弃
		if time.Since(pkg.Base.Time) > 5*time.Second {
			logx.Warnf("%s, elapsed: %v", method, time.Since(pkg.Base.Time))
			return rabbitmq.NackDiscard
		}
		handler, ok := r.reqHandler.Load(method)
		if !ok {
			// 未就绪, 丢到死信队列
			err := r.publisher.Publish(
				delivery.Body, []string{r.node + delayedSuffix},
				rabbitmq.WithPublishOptionsExchange(r.exchange),                                 // 交换机名称
				rabbitmq.WithPublishOptionsMandatory,                                            // 强制发布
				rabbitmq.WithPublishOptionsPersistentDelivery,                                   // 立即发布
				rabbitmq.WithPublishOptionsExpiration(fmt.Sprintf("%.f", expireTime.Seconds())), // 过期时间
			)
			if err != nil {
				logx.Errorf("publish %s to %s error: %v", method, r.node+delayedSuffix, err)
			}
			return rabbitmq.NackRequeue
		}

		data, err := handler.(HandlerFunc)(pkg.Data)
		res.Data = data
		if err != nil {
			res.Error = err.Error()
		}
	}

	if err := r.send(pkg.Base.RoutingKey, res); err != nil {
		logx.Errorf("failed to send response for %s: %v", method, err)
		return rabbitmq.NackDiscard
	}
	return rabbitmq.Ack
}

func (r *rpc) handlerRsp(delivery rabbitmq.Delivery) rabbitmq.Action {
	var pkg = new(response)
	if err := r.unpack(delivery.Body, pkg); err != nil {
		return rabbitmq.NackDiscard
	}

	handlerKey := strings.ToLower(pkg.Base.Method + pkg.Base.ID)
	channel, ok := r.rspHandler.Load(handlerKey)
	if !ok {
		// 将丢弃
		return rabbitmq.NackDiscard
	}
	select {
	case channel.(chan response) <- *pkg:
	default:
		logx.Warnf("Response channel is full for %s", handlerKey)
	}
	return rabbitmq.Ack
}
