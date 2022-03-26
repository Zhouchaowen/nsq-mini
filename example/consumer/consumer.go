// gin-vue/consumer.go
package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"nsq-mini/example/common"
	"os"
	"os/signal"
)

func doConsumerTask() {
	conn, err := net.Dial("tcp", "0.0.0.0:4150")
	if err != nil {
		panic(err)
	}

	go func() {
		for {
			var msgSize int32

			// message size
			err := binary.Read(conn, binary.BigEndian, &msgSize)
			if err != nil {
				fmt.Println(err.Error())
			}

			if msgSize < 0 {
				fmt.Printf("response msg size is negative: %v\n", msgSize)
			}
			// message binary data
			buf := make([]byte, msgSize)
			_, err = io.ReadFull(conn, buf)
			if err != nil {
				fmt.Println(err.Error())
			}
			fmt.Println("resp: ", string(buf))
		}
	}()

	conn.Write([]byte("  V2"))

	cmd := &common.Command{Name: []byte("IDENTIFY"), Body: []byte("")}
	_, err = cmd.WriteTo(conn)
	if err != nil {
		fmt.Println("send IDENTIFY err ", err.Error())
	}

	var params = [][]byte{[]byte("one-test"), []byte("ch-one-test1")}
	cmd = &common.Command{Name: []byte("SUB"), Params: params}
	_, err = cmd.WriteTo(conn)
	if err != nil {
		fmt.Println("send SUB err ", err.Error())
	}
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
}

func main() {
	doConsumerTask()
}
