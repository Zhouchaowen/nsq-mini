package nsqd

import (
	"fmt"
	"net"
)

type NSQD struct {
	tcpServer     *tcpServer
	tcpListener   net.Listener
	topicMap map[string]*Topic
}

func New(opts *Options) (*NSQD,error) {
	var err error

	nsq := &NSQD{
		topicMap: make(map[string]*Topic),
	}
	nsq.tcpServer = &tcpServer{}
	nsq.tcpListener,err = net.Listen("tcp",opts.TCPAddress)
	if err != nil {
		return nil, fmt.Errorf("listen (%s) failed - %s", opts.TCPAddress, err)
	}
	return nsq, nil
}


func (n *NSQD) Main() error {

	return nil
}

func (n *NSQD) Exit() {

}