package nsqd

type Channel struct {
	topicName string // Topic的名称
	name      string // Channel的名称

	backend BackendQueue // 消息优先存入这个内存chan

	memoryMsgChan chan *Message // 磁盘队列，当内存memoryMsgChan满时，写入硬盘队列
}