package amqprpc

import "time"

const (
	defaultExchange    = "rpc"
	defaultConcurrency = 15
	maxConcurrency     = 1024
	expireTime         = 1 * time.Second

	ActionShakehand = "shakehand"
	ActionCall      = "call"

	handshakeTimeout = 300 * time.Millisecond
	handshakeReqData = "shakehand_req"
	handshakeAckData = "shakehand_ack"

	routingKeySuffixReq = ".req"
	routingKeySuffixRsp = ".rsp"
	delayedSuffix       = ".delayed"
)
