package relay

type RedisQueueConsumer interface {
	Consume(delivery Delivery)
}

type RedisQueueBatchConsumer interface {
	Consume(deliveries []Delivery)
}
