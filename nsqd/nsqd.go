package nsqd

import (
	"fmt"
	"log"
	"net"
	"nsq-mini/internal/protocol"
	"sync"
)

type NSQD struct {
	tcpServer   *tcpServer
	tcpListener net.Listener
	topicMap    map[string]*Topic
}

func New(opts *Options) (*NSQD, error) {
	var err error

	nsq := &NSQD{
		topicMap: make(map[string]*Topic),
	}
	nsq.tcpServer = &tcpServer{nsqd: nsq}
	nsq.tcpListener, err = net.Listen("tcp", opts.TCPAddress)
	if err != nil {
		return nil, fmt.Errorf("listen (%s) failed - %s", opts.TCPAddress, err)
	}
	return nsq, nil
}

func (n *NSQD) Main() error {
	exitCh := make(chan error)
	var once sync.Once
	exitFunc := func(err error) {
		once.Do(func() {
			if err != nil {
				log.Printf(err.Error())
			}
			exitCh <- err
		})
	}

	go exitFunc(protocol.TCPServer(n.tcpListener, n.tcpServer))

	err := <-exitCh
	return err
}

func (n *NSQD) Exit() {

}

func (n *NSQD) GetTopic(topicName string) *Topic {
	// 很可能我们已经有了这个主题，所以先尝试读锁
	t, ok := n.topicMap[topicName]
	if ok {
		return t
	}

	t = NewTopic(topicName, n) // 创建一个Topic，开启messagePump，等待start。
	n.topicMap[topicName] = t  // 存到nsqd.topicMap中

	return t
}
