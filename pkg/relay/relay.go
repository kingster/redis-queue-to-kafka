package relay

import (
	"fmt"
	"time"
	"github.com/adjust/uniuri"
	"os"
	"os/signal"
)

type KafkaSink struct {
	Brokers string
}

type Relayer struct {
	Source RedisSource
	Sink   KafkaSink
}

type TaskConsumer struct {

}

func (consumer *TaskConsumer) Consume(delivery Delivery) {
	fmt.Println(delivery.Payload())
	// handle error
	//delivery.Reject()

	// perform task
	time.Sleep(time.Second)
	delivery.Ack()

}

func (r Relayer) Start() string {

	source := r.createClient()
	queues, err := r.getQueues(source)

	var taskQueue *redisQueue

	if err != nil {
		panic(err)
	}

	fmt.Println("queues :", queues)


	go func() {
		sigchan := make(chan os.Signal, 10)
		signal.Notify(sigchan, os.Interrupt)
		<-sigchan
		fmt.Println("Program killed !")

		if taskQueue != nil {
			taskQueue.StopConsuming()
		}

		// do last actions and wait for all write operations to end

		os.Exit(0)
	}()


	for _, element := range queues {
		// element is the element from someSlice for where we are
		fmt.Println(element)

		taskQueue = newQueue("redis-queue",fmt.Sprintf("connection-%s", hostname()), element, nil, source)
		for i := 1; i <= 0; i++ {
			taskQueue.Publish(fmt.Sprintf("%d---%s", i,  uniuri.NewLen(100)))
		}

		taskQueue.ReturnAllUnacked()
		taskQueue.StartConsuming(10, 500*time.Millisecond)

		taskConsumer := &TaskConsumer{}
		taskQueue.AddConsumer("task consumer", taskConsumer)

	}

	select {}


	return ""
}

func hostname() string {
	name, err := os.Hostname()
	if err != nil {
		return "default-hostname"
	} else {
		return name
	}

}