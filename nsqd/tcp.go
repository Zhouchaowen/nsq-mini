package nsqd

import (
	"io"
	"log"
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

	p.conns.Store(conn.RemoteAddr(), conn)

	client := prot.NewClient(conn)
	err = prot.IOLoop(client)
	if err != nil {
		log.Printf("client(%s) - %s", conn.RemoteAddr(), err)
	}

	p.conns.Delete(conn.RemoteAddr())
	client.Close()
}

func (p *tcpServer) Close() {
	p.conns.Range(func(k, v interface{}) bool {
		v.(protocol.Client).Close()
		return true
	})
}
