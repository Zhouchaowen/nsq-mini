package protocol

import (
	"fmt"
	"log"
	"net"
	"runtime"
	"strings"
	"sync"
)

type TCPHandler interface {
	Handle(net.Conn)
}

func TCPServer(listener net.Listener, handler TCPHandler) error {
	var wg sync.WaitGroup

	for { // 监听连接
		clientConn, err := listener.Accept()
		if err != nil {
			if nerr, ok := err.(net.Error); ok && nerr.Temporary() {
				runtime.Gosched()
				continue
			}
			// theres no direct way to detect this error because it is not exposed
			if !strings.Contains(err.Error(), "use of closed network connection") {
				return fmt.Errorf("listener.Accept() error - %s", err)
			}
			log.Printf(err.Error())
			break
		}

		wg.Add(1)
		go func() {
			handler.Handle(clientConn) // 每个连接启动一个go协程处理
			wg.Done()
		}()
	}

	// wait to return until all handler goroutines complete
	wg.Wait()

	return nil
}
