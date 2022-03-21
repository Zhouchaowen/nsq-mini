package nsqd

import (
	"log"
	"time"
)

type Topic struct {
	name          string              // topic名，生产和消费时需要指定此名称
	channelMap    map[string]*Channel // 保存每个channel name和channel指针的映射
	memoryMsgChan chan *Message       // 消息优先存入这个内存chan

	idFactory *guidFactory // id生成器工厂
}

// NewTopic Topic constructor
func NewTopic(topicName string, nsqd *NSQD) *Topic {
	t := &Topic{
		name:          topicName,
		channelMap:    make(map[string]*Channel),
		memoryMsgChan: make(chan *Message, 100),
		idFactory:     NewGUIDFactory(1), // 全局id生成工厂
	}

	go t.messagePump()
	return t
}

func (t *Topic) messagePump() {
	var msg *Message
	var memoryMsgChan chan *Message
	memoryMsgChan = t.memoryMsgChan
	// main message loop
	for {
		if len(t.channelMap) == 0 {
			time.Sleep(time.Second)
			continue
		}

		select { // 从这里可以看到，如果消息已经被写入磁盘的话，nsq 消费消息就是无序的
		case msg = <-memoryMsgChan: // 从内存channel获取msg
		}

		for _, channel := range t.channelMap {
			log.Printf("Topic send msg to channle: %v ", msg)
			err := channel.PutMessage(msg) // 发送msg到内存Channel或持久化队列
			if err != nil {
				log.Fatalf("TOPIC(%s) ERROR: failed to put msg(%s) to channel(%s) - %s", t.name, msg.ID, channel.name, err)
			}
		}
	}

	log.Fatalf("TOPIC(%s): closing ... messagePump", t.name)
}

// 添加消息到内存队列或持久化队列（写入 memoryMsgChan 或者 BackendQueue）
func (t *Topic) put(m *Message) error {
	select {
	case t.memoryMsgChan <- m: // 当 chan 中还能写入时，写入 memoryMsgChan
	default: // 否则，写入 BackendQueue，落入磁盘
		//TODO 添加消息到持久化队列
	}
	return nil
}

// PutMessage 向 topic 写一条消息，如果 memoryMsgQueue 满了，则写入 backendQueue
func (t *Topic) PutMessage(m *Message) error {
	// 添加消息到内存队列或持久化队列
	if err := t.put(m); err != nil {
		log.Fatalf("put msg to Topic err %s", err.Error())
		return err
	}
	return nil
}

func (t *Topic) GetChannel(channelName string) *Channel {
	channel, _ := t.getOrCreateChannel(channelName)

	return channel
}

// this expects the caller to handle locking
func (t *Topic) getOrCreateChannel(channelName string) (*Channel, bool) {
	channel, ok := t.channelMap[channelName]
	if !ok {

		channel = NewChannel(t.name, channelName) // 创建一个Channel
		t.channelMap[channelName] = channel       // 保存到Topic.channelMap
		log.Printf("TOPIC(%s): new channel(%s)", t.name, channel.name)
		return channel, true
	}
	return channel, false
}

func (t *Topic) GenerateID() MessageID {
	var i int64 = 0
	for {
		id, err := t.idFactory.NewGUID()
		if err == nil {
			return id.Hex()
		}
		if i%10000 == 0 {
			log.Fatalf("TOPIC(%s): failed to create guid - %s", t.name, err)
		}
		time.Sleep(time.Millisecond)
		i++
	}
}
