package relay

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
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

func (r Relayer) shutdownHandler(stopped chan bool) {
	sigchan := make(chan os.Signal, 10)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)
	<-sigchan

	for _, taskQueue := range r.sourceQueues {
		fmt.Println("Shutdown Triggered.... StopConsuming !")
		taskQueue.StopConsuming()
	}

	stopped <- true
}

func (r Relayer) Run() {

	source := r.createClient()
	queues, err := r.getQueues(source)
	sink, err := r.createProducer()
	stopped := make(chan bool)

	if err != nil {
		panic(err)
	}

	fmt.Println("queues :", queues)

	if len(queues) <= 0 {
		fmt.Println("No Queues Found, Shutting Down!")
		os.Exit(0)
	}

	go r.shutdownHandler(stopped)

	for _, queueName := range queues {
		fmt.Println(queueName)

		taskQueue := newQueue("redis-queue", fmt.Sprintf("connection-%s", hostname()), queueName, nil, source)
		r.sourceQueues = append(r.sourceQueues, taskQueue)

		taskQueue.ReturnAllUnacked()
		taskQueue.StartConsuming(r.Options.SourceBatchSize, 500*time.Millisecond)

		taskKafkaPublisher := &KafkaPublisher{sink, queueName}
		taskKafkaPublisher.HandleDelivery()
		taskQueue.AddBatchConsumer("task-batch-consumer", r.Options.SinkBatchSize, taskKafkaPublisher)

	}

	<-stopped
	fmt.Println("Shutdown Consumers")
}

func hostname() string {
	name, err := os.Hostname()
	if err != nil {
		return "default-hostname"
	} else {
		return name
	}
}
