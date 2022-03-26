// gin-vue/product.go
package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"nsq-mini/example/common"
	"os"
	"os/signal"
	"strconv"
)

func sendMessage() {
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

	for i := 0; i < 5; i++ { // 4. 生产者发布消息
		message := "消息发送测试 " + strconv.Itoa(i+10000)
		var params = [][]byte{[]byte("one-test")}
		cmd = &common.Command{Name: []byte("PUB"), Params: params, Body: []byte(message)}
		_, err = cmd.WriteTo(conn)
		if err != nil {
			fmt.Println("send PUB err ", err.Error())
		}
	}
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
}

func main() {
	sendMessage()
}
