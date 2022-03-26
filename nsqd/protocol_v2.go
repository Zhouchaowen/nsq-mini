package nsqd

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"nsq-mini/internal/protocol"
	"time"
)

const (
	frameTypeResponse int32 = 0
	frameTypeError    int32 = 1
	frameTypeMessage  int32 = 2
)

var separatorBytes = []byte(" ")           // tcp 字节流参数分隔符
var heartbeatBytes = []byte("_heartbeat_") // 心跳包 返回
var okBytes = []byte("OK")

type protocolV2 struct {
	nsqd *NSQD
}

func (p *protocolV2) NewClient(conn net.Conn) protocol.Client {
	return newClientV2(conn, p.nsqd)
}

func (p *protocolV2) IOLoop(c protocol.Client) error {
	var err error
	var line []byte

	client := c.(*clientV2)

	messagePumpStartedChan := make(chan bool)
	go p.messagePump(client, messagePumpStartedChan)
	<-messagePumpStartedChan

	for {
		line, err = client.Reader.ReadSlice('\n') // 解析请求参数
		if err != nil {
			if err == io.EOF {
				err = nil
			} else {
				err = fmt.Errorf("failed to read command - %s", err)
			}
			break
		}

		// trim the '\n'
		line = line[:len(line)-1]
		// optionally trim the '\r'
		if len(line) > 0 && line[len(line)-1] == '\r' {
			line = line[:len(line)-1]
		}
		params := bytes.Split(line, separatorBytes)

		var response []byte
		response, err = p.Exec(client, params) // 调用相应的 handler 处理 tcp request
		log.Printf(string(response))
		if err != nil {
			sendErr := p.Send(client, frameTypeError, []byte(err.Error()))
			if sendErr != nil {
				break
			}
			continue
		}
		if response != nil {
			err = p.Send(client, frameTypeResponse, response) // 响应
			if err != nil {
				err = fmt.Errorf("failed to send response - %v", err)
				break
			}
		}
	}

	log.Println("close client.")
	close(client.ExitChan)

	return err
}

func (p *protocolV2) messagePump(client *clientV2, startedChan chan bool) {
	var err error
	var memoryMsgChan chan *Message
	var subChannel *Channel // 订阅Channel

	//heartbeatTicker := time.NewTicker(3)      // 心跳 定时器
	//heartbeatChan := heartbeatTicker.C

	// signal to the goroutine that started the messagePump
	// that we've started up
	// 向启动我们的 messagePump 的 goroutine 发出信号
	close(startedChan)

	for {
		if client.Channel != nil {
			memoryMsgChan = client.Channel.memoryMsgChan
			subChannel = client.Channel
		}

		select {
		//case <-heartbeatChan: // 接收心跳通知，发送心跳
		//	err = p.Send(client, frameTypeResponse, heartbeatBytes)
		//	if err != nil {
		//		goto exit
		//	}
		case msg := <-memoryMsgChan: // 从 channel 的内存 memoryMsgChan 中消费一条消息
			log.Printf("channel send msg to client")
			subChannel.StartInFlightTimeout(msg) // 添加到In-Flight
			err = p.SendMessage(client, msg)     // 发送消息
			if err != nil {
				goto exit
			}
		case <-client.ExitChan: // 接收退出通知
			goto exit
		default:
			client.writeLock.Lock()
			err = client.Flush()
			client.writeLock.Unlock()
			if err != nil {
				goto exit
			}
			time.Sleep(3 * time.Second)
			log.Printf("wait register channel")
		}
	}

exit:
	log.Printf("PROTOCOL(V2): [%v] exiting messagePump\n", client)
	if err != nil {
		log.Printf("PROTOCOL(V2): [%v] messagePump error - %s", client, err)
	}
}

// SendMessage 将 channel 中的消息发送给 consumer client
func (p *protocolV2) SendMessage(client *clientV2, msg *Message) error {
	log.Printf("PROTOCOL(V2): writing msg(%s) to client(%v)", msg.ID, client)

	var buf = &bytes.Buffer{}

	_, err := msg.WriteTo(buf)
	if err != nil {
		return err
	}

	err = p.Send(client, frameTypeMessage, buf.Bytes())
	if err != nil {
		return err
	}
	client.Flush()
	return nil
}

func (p *protocolV2) Send(client *clientV2, frameType int32, data []byte) error {
	client.writeLock.Lock()
	_, err := protocol.SendFramedResponse(client.Writer, frameType, data)
	if err != nil {
		client.writeLock.Unlock()
		return err
	}

	// 如果帧类型不是消息类型，flush writer buffer，立即发送
	if frameType != frameTypeMessage {
		err = client.Flush()
	}
	client.writeLock.Unlock()
	return err
}

func (p *protocolV2) Exec(client *clientV2, params [][]byte) ([]byte, error) {
	if bytes.Equal(params[0], []byte("IDENTIFY")) { // client 注册
		return p.IDENTIFY(client, params)
	}

	switch {
	case bytes.Equal(params[0], []byte("FIN")): // 一条消息消费完毕
		return p.FIN(client, params)
	case bytes.Equal(params[0], []byte("RDY")): // consumer 确认当前接收消息能力
		return p.RDY(client, params)
	case bytes.Equal(params[0], []byte("REQ")): // 重新消费
		return p.REQ(client, params)
	case bytes.Equal(params[0], []byte("PUB")): // producer 发布一条消息
		return p.PUB(client, params)
	case bytes.Equal(params[0], []byte("DPUB")): // producer 延迟发布消息
		return p.DPUB(client, params)
	case bytes.Equal(params[0], []byte("NOP")): // 心跳检测
		return p.NOP(client, params)
	case bytes.Equal(params[0], []byte("SUB")): // consumer 订阅 channel
		return p.SUB(client, params)
	case bytes.Equal(params[0], []byte("CLS")): // 关闭
		return p.CLS(client, params)
	}
	return okBytes, nil
}

func (p *protocolV2) IDENTIFY(client *clientV2, params [][]byte) ([]byte, error) {
	lenSlice := make([]byte, 4)
	bodyLen, err := readLen(client.Reader, lenSlice)
	if err != nil {
		return nil, fmt.Errorf("IDENTIFY failed to read body size")
	}

	// 如果包长小于 0，报错
	if bodyLen <= 0 {
		return nil, fmt.Errorf("IDENTIFY invalid body size %d", bodyLen)
	}

	body := make([]byte, bodyLen)
	_, err = io.ReadFull(client.Reader, body)

	log.Println("receive IDENTIFY ", string(body))
	log.Println("set config: contraction,heartbeat,RAD........")
	return okBytes, nil
}

func (p *protocolV2) FIN(client *clientV2, params [][]byte) ([]byte, error) {
	return okBytes, nil
}

func (p *protocolV2) RDY(client *clientV2, params [][]byte) ([]byte, error) {
	return okBytes, nil
}

func (p *protocolV2) REQ(client *clientV2, params [][]byte) ([]byte, error) {
	return okBytes, nil
}

func (p *protocolV2) PUB(client *clientV2, params [][]byte) ([]byte, error) {
	if len(params) < 2 {
		return nil, fmt.Errorf("PUB insufficient number of parameters")
	}

	topicName := string(params[1])

	lenSlice := make([]byte, 4)
	bodyLen, err := readLen(client.Reader, lenSlice)
	if err != nil {
		return nil, fmt.Errorf("PUB failed to read message body size")
	}

	if bodyLen <= 0 {
		return nil, fmt.Errorf("PUB invalid message body size %d", bodyLen)
	}

	messageBody := make([]byte, bodyLen) // 读取body
	_, err = io.ReadFull(client.Reader, messageBody)
	if err != nil {
		return nil, fmt.Errorf("PUB failed to read message body")
	}

	topic := p.nsqd.GetTopic(topicName)
	msg := NewMessage(topic.GenerateID(), messageBody)
	err = topic.PutMessage(msg) // 添加消息到内存队列或持久化队列并更新统计信息
	if err != nil {
		return nil, fmt.Errorf("PUB failed " + err.Error())
	}

	return okBytes, nil
}

func (p *protocolV2) DPUB(client *clientV2, params [][]byte) ([]byte, error) {
	return okBytes, nil
}

func (p *protocolV2) NOP(client *clientV2, params [][]byte) ([]byte, error) {
	return okBytes, nil
}

func (p *protocolV2) SUB(client *clientV2, params [][]byte) ([]byte, error) {
	if len(params) < 3 {
		return nil, fmt.Errorf("SUB insufficient number of parameters")
	}

	topicName := string(params[1])
	channelName := string(params[2])

	var channel *Channel
	topic := p.nsqd.GetTopic(topicName)
	channel = topic.GetChannel(channelName)
	client.Channel = channel
	// update message pump 更新消息泵
	return okBytes, nil
}

func (p *protocolV2) CLS(client *clientV2, params [][]byte) ([]byte, error) {
	return okBytes, nil
}

// 读取 tcp body 的长度
func readLen(r io.Reader, tmp []byte) (int32, error) {
	_, err := io.ReadFull(r, tmp)
	if err != nil {
		return 0, err
	}
	return int32(binary.BigEndian.Uint32(tmp)), nil
}
