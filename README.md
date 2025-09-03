# amqprpc
🐰 Framework to use RabbitMQ as RPC

## Usage

### program app 1

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/busyster996/amqprpc"
	"github.com/busyster996/amqprpc/pkg/logx"
	"github.com/busyster996/amqprpc/pkg/rabbitmq"
)

func main() {
	conn, err := rabbitmq.NewConn(
		"amqp://admin:123456@localhost:5672/",
		rabbitmq.WithConnectionOptionsLogger(logx.GetSubLogger()), // 日志
	)
	if err != nil {
		logx.Fatalln(err)
	}

	rpc, err := amqprpc.New(
		context.Background(), conn,
		amqprpc.WithNode("program1"),
	)
	if err != nil {
		logx.Errorln(err)
		return
	}
	defer rpc.Close()

	for {
		res, err := rpc.CallWithTimeout(3*time.Second, "program2", "add", nil)
		if err != nil {
			logx.Warnln(err)
		}
		fmt.Println("111111111", string(res))
		time.Sleep(300 * time.Millisecond)
	}
}
```

### program app 2

```go
package main

import (
	"context"

	"github.com/busyster996/amqprpc"
	"github.com/busyster996/amqprpc/pkg/logx"
	"github.com/busyster996/amqprpc/pkg/rabbitmq"
)

func main() {
	conn, err := rabbitmq.NewConn(
		"amqp://admin:123456@localhost:5672/",
		rabbitmq.WithConnectionOptionsLogger(logx.GetSubLogger()), // 日志
	)
	if err != nil {
		logx.Fatalln(err)
	}

	rpc, err := amqprpc.New(
		context.Background(), conn,
		amqprpc.WithNode("program2"),
	)
	if err != nil {
		logx.Errorln(err)
		return
	}
	defer rpc.Close()

	_ = rpc.RegisterHandler("add", func(data []byte) ([]byte, error) {
		logx.Infoln(rpc.GetNode(), "add")
		return []byte("hello world"), nil
	})

	select {}
}
```