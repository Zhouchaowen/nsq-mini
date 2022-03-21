package nsqd

type Topic struct {
	name              string                // topic名，生产和消费时需要指定此名称
	channelMap        map[string]*Channel   // 保存每个channel name和channel指针的映射
	memoryMsgChan     chan *Message         // 消息优先存入这个内存chan
}