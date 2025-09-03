package amqprpc

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/rabbitmq/amqp091-go"
	"github.com/segmentio/ksuid"

	"github.com/busyster996/amqprpc/pkg/logx"
	"github.com/busyster996/amqprpc/pkg/rabbitmq"
)

type HandlerFunc func(data []byte) ([]byte, error)

type IRPC interface {
	GetNode() string
	GetExchange() string
	// Close the RPC server.
	Close()
	// UnregisterHandler unregisters a handler for a given method.
	UnregisterHandler(method string) error
	// RegisterHandler registers a handler for a given method.
	RegisterHandler(method string, handler HandlerFunc) error
	// Call calls a remote method with the given data.
	Call(ctx context.Context, node, method string, data []byte) ([]byte, error)
	// CallWithTimeout calls a remote method with the given data and timeout.
	CallWithTimeout(timeout time.Duration, node, method string, data []byte) ([]byte, error)
}

type rpc struct {
	concurrency int
	node        string
	exchange    string
	reqHandler  sync.Map
	rspHandler  sync.Map
	wg          sync.WaitGroup
	ctx         context.Context
	conn        *rabbitmq.Conn
	publisher   *rabbitmq.Publisher
	consumer    *rabbitmq.Consumer
}

func New(ctx context.Context, conn *rabbitmq.Conn, options ...Option) (IRPC, error) {
	node, err := os.Hostname()
	if err != nil {
		node = ksuid.New().String()
	}
	r := &rpc{
		ctx:         ctx,
		node:        node,
		exchange:    defaultExchange,
		reqHandler:  sync.Map{},
		rspHandler:  sync.Map{},
		conn:        conn,
		concurrency: defaultConcurrency,
	}
	for _, option := range options {
		if err = option(r); err != nil {
			return nil, err
		}
	}
	return r, r.start()
}

func (r *rpc) start() (err error) {
	if err = r.declareDelayedQueue(); err != nil {
		return err
	}
	r.publisher, err = r.createPublisher()
	if err != nil {
		logx.Errorln(err)
		return err
	}

	r.consumer, err = r.newConsumer(
		r.node,
		r.node+routingKeySuffixRsp,
		r.node+routingKeySuffixReq,
	)
	if err != nil {
		return err
	}

	logx.Infoln("RPC server started successfully")
	return
}

func (r *rpc) declareDelayedQueue() error {
	delayedQueue := r.node + delayedSuffix
	channel, err := rabbitmq.NewChannel(r.conn, rabbitmq.WithChannelOptionsLogger(logx.GetSubLogger()))
	if err != nil {
		logx.Errorln(err)
		return err
	}
	defer channel.Close()
	if _, err = channel.QueueDeclareSafe(
		delayedQueue, true, false, false, false,
		amqp091.Table{
			"x-queue-type":              "quorum",
			"x-dead-letter-exchange":    r.exchange,
			"x-dead-letter-routing-key": r.node + routingKeySuffixReq,
		}); err != nil {
		logx.Errorln(err)
		return err
	}
	err = channel.QueueBindSafe(delayedQueue, delayedQueue, r.exchange, false, amqp091.Table{})
	if err != nil {
		logx.Errorln(err)
		return err
	}
	return nil
}

func (r *rpc) createPublisher() (*rabbitmq.Publisher, error) {
	return rabbitmq.NewPublisher(
		r.conn,
		rabbitmq.WithPublisherOptionsLogger(logx.GetSubLogger()),
		rabbitmq.WithPublisherOptionsExchangeName(r.exchange),
		rabbitmq.WithPublisherOptionsExchangeKind(amqp091.ExchangeDirect),
		rabbitmq.WithPublisherOptionsExchangeDeclare,
		rabbitmq.WithPublisherOptionsExchangeDurable,
		rabbitmq.WithPublisherOptionsConfirm,
	)
}

func (r *rpc) newConsumer(queue string, routingKeys ...string) (*rabbitmq.Consumer, error) {
	opts := []func(*rabbitmq.ConsumerOptions){
		rabbitmq.WithConsumerOptionsConcurrency(r.concurrency),           // 并发数
		rabbitmq.WithConsumerOptionsLogger(logx.GetSubLogger()),          // 日志
		rabbitmq.WithConsumerOptionsExchangeName(r.exchange),             // 交换机名称
		rabbitmq.WithConsumerOptionsExchangeKind(amqp091.ExchangeDirect), // 交换机类型
		rabbitmq.WithConsumerOptionsExchangeDeclare,                      // 声明交换机
		rabbitmq.WithConsumerOptionsExchangeDurable,                      // 交换机持久化
		rabbitmq.WithConsumerOptionsQueueDurable,                         // 队列持久化
		rabbitmq.WithConsumerOptionsQueueQuorum,                          // 使用仲裁队列
	}
	for _, routingKey := range routingKeys {
		opts = append(opts, rabbitmq.WithConsumerOptionsRoutingKey(routingKey))
	}
	consumer, err := rabbitmq.NewConsumer(
		r.conn, queue,
		opts...,
	)
	if err != nil {
		logx.Errorf("new %v/%s consumer error: %v", routingKeys, queue, err)
		return nil, err
	}
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		for {
			select {
			case <-r.ctx.Done():
				return
			default:
				err = consumer.Run(r.handler)
				if err != nil && !consumer.IsClosed() {
					logx.Errorf("%v/%s consumer run error: %v", routingKeys, queue, err)
					time.Sleep(300 * time.Millisecond)
				}
			}
		}
	}()
	time.Sleep(500 * time.Millisecond)
	return consumer, nil
}

func (r *rpc) GetNode() string {
	return r.node
}

func (r *rpc) GetExchange() string {
	return r.exchange
}

func (r *rpc) Close() {
	if r.consumer != nil {
		r.consumer.Close()
	}
	if r.consumer != nil {
		r.consumer.Close()
	}
	r.wg.Wait()
	if r.publisher != nil {
		r.publisher.Close()
	}
	return
}

func (r *rpc) UnregisterHandler(method string) error {
	method = strings.ToLower(method)
	_, ok := r.reqHandler.LoadAndDelete(method)
	if !ok {
		return ErrHandlerNotFound
	}
	logx.Infof("unregistered handler for method: %s", method)
	return nil
}

func (r *rpc) RegisterHandler(method string, handler HandlerFunc) error {
	method = strings.ToLower(method)
	_, loaded := r.reqHandler.LoadOrStore(method, handler)
	if loaded {
		return ErrHandlerExists
	}
	logx.Infof("registered handler for method: %s", method)
	return nil
}

func (r *rpc) unpack(data []byte, pkg any) error {
	err := json.Unmarshal(data, pkg)
	if err != nil {
		logx.Errorf("json unmarshal error: %v", err)
		return err
	}
	return nil
}

func (r *rpc) pack(pkg any) ([]byte, error) {
	data, err := json.Marshal(pkg)
	if err != nil {
		logx.Errorf("json marshal error: %v", err)
		return nil, err
	}
	return data, nil
}

func (r *rpc) send(routingKey string, pkg any) error {
	data, err := r.pack(pkg)
	if err != nil {
		return err
	}
	return r.publisher.Publish(
		data, []string{routingKey},
		rabbitmq.WithPublishOptionsExchange(r.exchange), // 交换机名称
		rabbitmq.WithPublishOptionsMandatory,            // 强制发布
		rabbitmq.WithPublishOptionsPersistentDelivery,   // 立即发布
	)
}

func (r *rpc) startHandshake(node, method, id string, errCh chan<- error) {
	err := r.sendShakehand(node, method, id)
	if err != nil {
		select {
		case errCh <- err:
		default:
		}
	}
}

func (r *rpc) sendShakehand(node, method, id string) error {
	return r.send(node+routingKeySuffixReq, &request{
		Base: base{
			ID:         id,
			Method:     method,
			Time:       time.Now(),
			RoutingKey: r.node + routingKeySuffixRsp,
		},
		Action: ActionShakehand,
		Data:   []byte(handshakeReqData),
	})
}
