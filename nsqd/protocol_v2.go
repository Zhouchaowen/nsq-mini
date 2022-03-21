package nsqd

import (
	"net"
	"nsq-mini/internal/protocol"
)

const (
	frameTypeResponse int32 = 0
	frameTypeError    int32 = 1
	frameTypeMessage  int32 = 2
)

type protocolV2 struct {
	nsqd *NSQD
}

func (p *protocolV2) NewClient(conn net.Conn) protocol.Client {
	return newClientV2(conn,p.nsqd)
}


func (p *protocolV2) IOLoop(c protocol.Client) error {

	return nil
}