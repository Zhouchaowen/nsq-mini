package nsqd

import (
	"io"
	"net"
	"nsq-mini/internal/protocol"
	"sync"
)

type tcpServer struct {
	nsqd  *NSQD
	conns sync.Map
}

func (p *tcpServer) Handle(conn net.Conn) {
	buf := make([]byte, 4)
	_, err := io.ReadFull(conn, buf)
	if err != nil {
		conn.Close()
		return
	}
	protocolMagic := string(buf)

	_ = protocolMagic

	var prot protocol.Protocol
	switch protocolMagic {
	case "  V2":
		prot = &protocolV2{nsqd: p.nsqd}
	default:
		protocol.SendFramedResponse(conn, frameTypeError, []byte("E_BAD_PROTOCOL"))
		conn.Close()
		return
	}

	client := prot.NewClient(conn)
	prot.IOLoop(client)

	client.Close()
}
