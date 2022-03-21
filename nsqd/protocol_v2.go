package nsqd

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"nsq-mini/internal/protocol"
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
				err = fmt.Errorf("failed to send response - %s", err)
				break
			}
		}
	}
	return nil
}

func (p *protocolV2) Send(client *clientV2, frameType int32, data []byte) error {
	_, err := protocol.SendFramedResponse(client.Writer, frameType, data)
	if err != nil {
		return err
	}

	// 如果帧类型不是消息类型，flush writer buffer，立即发送
	if frameType != frameTypeMessage {
		err = client.Flush()
	}

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
	return okBytes, nil
}

func (p *protocolV2) DPUB(client *clientV2, params [][]byte) ([]byte, error) {
	return okBytes, nil
}

func (p *protocolV2) NOP(client *clientV2, params [][]byte) ([]byte, error) {
	return okBytes, nil
}

func (p *protocolV2) SUB(client *clientV2, params [][]byte) ([]byte, error) {
	return okBytes, nil
}

func (p *protocolV2) CLS(client *clientV2, params [][]byte) ([]byte, error) {
	return okBytes, nil
}
