package nsqd

import (
	"errors"
	"log"
)

type Channel struct {
	topicName string // Topic的名称
	name      string // Channel的名称

	backend BackendQueue // 消息优先存入这个内存chan

	memoryMsgChan chan *Message // 磁盘队列，当内存memoryMsgChan满时，写入硬盘队列

	inFlightMessages map[MessageID]*Message // 保存已推送尚未收到FIN的消息
}

// NewChannel creates a new instance of the Channel type and returns a pointer
func NewChannel(topicName string, channelName string) *Channel {

	c := &Channel{
		topicName:        topicName,
		name:             channelName,
		memoryMsgChan:    make(chan *Message, 100),
		inFlightMessages: make(map[MessageID]*Message),
	}
	return c
}

// pushInFlightMessage atomically adds a message to the in-flight dictionary
func (c *Channel) pushInFlightMessage(msg *Message) error {
	_, ok := c.inFlightMessages[msg.ID]
	if ok {
		return errors.New("ID already in flight")
	}
	c.inFlightMessages[msg.ID] = msg
	return nil
}

// StartInFlightTimeout 将一条消息写入 inFlight 队列
func (c *Channel) StartInFlightTimeout(msg *Message) error {
	err := c.pushInFlightMessage(msg)
	if err != nil {
		return err
	}
	return nil
}

// PutMessage writes a Message to the queue
func (c *Channel) PutMessage(m *Message) error {
	if err := c.put(m); err != nil {
		return err
	}
	return nil
}

// 将消息发送到Channel.memoryMsgChan或持久化队列Channel.backend
func (c *Channel) put(m *Message) error {
	select {
	case c.memoryMsgChan <- m: // 内存Channel
	default: // TODO 持久化队列
		log.Printf("read msg from backend")
	}
	return nil
}
