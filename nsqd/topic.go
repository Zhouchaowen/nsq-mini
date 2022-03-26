package nsqd

import (
	"log"
	"sync"
	"time"
)

type Topic struct {
	name              string              // topic名，生产和消费时需要指定此名称
	channelMap        map[string]*Channel // 保存每个channel name和channel指针的映射
	memoryMsgChan     chan *Message       // 消息优先存入这个内存chan
	channelUpdateChan chan int            // topic 内的 channel map 增加/删除的更新通知
	exitChan          chan int            // 终止 chan

	idFactory *guidFactory // id生成器工厂

	sync.RWMutex
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
	var chans []*Channel

	// do not pass messages before Start(), but avoid blocking Pause() or GetChannel()
	for { // TODO
		select {
		case <-t.channelUpdateChan: // 接收 Channel 更新通知 （创建或删除）
			continue
		case <-t.exitChan: // 接收退出通知
			goto exit
		}
		break
	}
	t.RLock()
	for _, c := range t.channelMap {
		chans = append(chans, c)
	}
	t.RUnlock()
	if len(chans) > 0 { // TODO Topic是没有暂停
		memoryMsgChan = t.memoryMsgChan
	}

	// main message loop
	for {
		select { // 从这里可以看到，如果消息已经被写入磁盘的话，nsq 消费消息就是无序的
		case <-t.channelUpdateChan: // 接收 Channel 更新通知 （创建或删除）
			chans = chans[:0]
			t.RLock()
			for _, c := range t.channelMap {
				chans = append(chans, c)
			}
			t.RUnlock()
			if len(chans) == 0 {
				memoryMsgChan = nil
			} else {
				memoryMsgChan = t.memoryMsgChan
			}
			continue
		case msg = <-memoryMsgChan: // 从内存channel获取msg
		}

		for _, channel := range chans {
			log.Printf("Topic send msg to channle: %v ", msg)
			err := channel.PutMessage(msg) // 发送msg到内存Channel或持久化队列
			if err != nil {
				log.Fatalf("TOPIC(%s) ERROR: failed to put msg(%s) to channel(%s) - %s", t.name, msg.ID, channel.name, err)
			}
		}
	}
exit:
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
	t.Lock()
	channel, isNew := t.getOrCreateChannel(channelName)
	t.Unlock()
	if isNew {
		// update messagePump state
		select {
		case t.channelUpdateChan <- 1: // Channel 更新通知
		case <-t.exitChan:
		}
	}

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
