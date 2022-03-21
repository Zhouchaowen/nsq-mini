package main

import (
	"flag"
	"nsq-mini/nsqd"
	"os"
	"os/signal"
	"sync"
)

type program struct {
	once sync.Once
	nsqd *nsqd.NSQD
}

var TCPAddress string

func init() {
	flag.String("tcp-address", TCPAddress, "<addr>:<port> to listen on for TCP clients")
}

func main() {
	opts := nsqd.NewOptions()
	if TCPAddress != "" {
		opts.TCPAddress = TCPAddress
	}

	prg := &program{}
	nsqd, err := nsqd.New(opts)
	if err != nil {
		panic(err)
	}
	prg.nsqd = nsqd

	prg.Start()

}

func (p *program) Start() error {
	go func() {
		err := p.nsqd.Main()
		if err != nil {
			p.Stop()
			os.Exit(1)
		}
	}()
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
	return nil
}

func (p *program) Stop() error {
	p.once.Do(func() {
		p.nsqd.Exit()
	})
	return nil
}
