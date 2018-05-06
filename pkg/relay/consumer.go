package relay

type RedisQueueConsumer interface {
	Consume(delivery Delivery)
}
