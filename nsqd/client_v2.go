package nsqd

import (
	"bufio"
	"net"
)

const defaultBufferSize = 16 * 1024

// 保存每个客户端的连接信息。
type clientV2 struct {
	ID        int64 // client ID
	nsqd      *NSQD // 上下文作用域，存放当前 nsqd
	UserAgent string

	// original connection
	net.Conn // client 和 nsqd server 的连接

	// reading/writing interfaces
	Reader *bufio.Reader
	Writer *bufio.Writer
}


func newClientV2( conn net.Conn, nsqd *NSQD) *clientV2 {
	c := &clientV2{
		nsqd: nsqd,
		Conn: conn,
		Reader: bufio.NewReaderSize(conn, defaultBufferSize),
		Writer: bufio.NewWriterSize(conn, defaultBufferSize),
	}
	return c
}
