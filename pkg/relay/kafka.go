package relay

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"fmt"
)
type KafkaSink struct {
	Brokers string
}

func (r Relayer) createProducer() (*kafka.Producer, error) {
	producer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": r.Sink.Brokers})
	if err != nil {
		return nil,err
	}
	return producer, nil
}

type KafkaPublisher struct {
	producer          *kafka.Producer
	topic             string
}

func (publisher *KafkaPublisher) HandleDelivery(){
	go publisher.deliveryHandler()
}

func (publisher *KafkaPublisher) deliveryHandler() {
	for e := range publisher.producer.Events() {
		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
				//reject message?
			} else {
				fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
				ev.Opaque.(Delivery).Ack()
			}
		}
	}
}


func (publisher *KafkaPublisher) Consume(deliveries []Delivery) {

	// perform task
	for _, element := range deliveries {
		publisher.producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &publisher.topic, Partition: kafka.PartitionAny},
			Value:   []byte(element.Payload()),
			Opaque:  element,
		}, nil)
	}

	publisher.producer.Flush(15 * 1000)

}

//KafkaPublisher impl RedisQueueBatchConsumer
var _ RedisQueueBatchConsumer = (*KafkaPublisher)(nil)