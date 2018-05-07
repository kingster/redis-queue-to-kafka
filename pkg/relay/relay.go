package relay

import (
	"fmt"
	"github.com/adjust/uniuri"
	"os"
	"os/signal"
	"time"
)

type Options struct {
	SourceBatchSize int
	SinkBatchSize   int
}

type Relayer struct {
	Source       RedisSource
	Sink         KafkaSink
	Options      *Options
	sourceQueues []*redisQueue
}

func (r Relayer) shutdownHandler() {
	sigchan := make(chan os.Signal, 10)
	signal.Notify(sigchan, os.Interrupt)
	<-sigchan

	for _, taskQueue := range r.sourceQueues {
		fmt.Println("Shutdown Triggered.... StopConsuming !")
		taskQueue.StopConsuming()
	}

	time.Sleep(time.Second)
	os.Exit(0)
}

func (r Relayer) Run() {

	source := r.createClient()
	queues, err := r.getQueues(source)
	sink, err := r.createProducer()

	if err != nil {
		panic(err)
	}

	fmt.Println("queues :", queues)

	go r.shutdownHandler()

	for _, queueName := range queues {
		fmt.Println(queueName)

		taskQueue := newQueue("redis-queue", fmt.Sprintf("connection-%s", hostname()), queueName, nil, source)
		r.sourceQueues = append(r.sourceQueues, taskQueue)
		for i := 1; i <= 1000; i++ {
			taskQueue.Publish(fmt.Sprintf("%d---%s", i, uniuri.NewLen(100)))
		}

		taskQueue.ReturnAllUnacked()
		taskQueue.StartConsuming(r.Options.SourceBatchSize, 500*time.Millisecond)

		//taskConsumer := &TaskConsumer{}
		//taskQueue.AddConsumer("task consumer", taskConsumer)

		taskKafkaPublisher := &KafkaPublisher{sink, queueName}
		taskKafkaPublisher.HandleDelivery()
		taskQueue.AddBatchConsumer("task-batch-consumer", r.Options.SinkBatchSize, taskKafkaPublisher)

	}

	select {}
}

func hostname() string {
	name, err := os.Hostname()
	if err != nil {
		return "default-hostname"
	} else {
		return name
	}
}
