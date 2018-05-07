package main

import (
	"./pkg/relay"
	"fmt"
)

func main() {

	fmt.Println(`
	.▄▄ · ▄▄▄▄▄▄▄▄        ▄▄▌ ▐ ▄▌ ▄▄ • ▄▄▄ .▄▄▄
▐█ ▀. •██  ▀▄ █·▪     ██· █▌▐█▐█ ▀ ▪▀▄.▀·▀▄ █·
▄▀▀▀█▄ ▐█.▪▐▀▀▄  ▄█▀▄ ██▪▐█▐▐▌▄█ ▀█▄▐▀▀▪▄▐▀▀▄
▐█▄▪▐█ ▐█▌·▐█•█▌▐█▌.▐▌▐█▌██▐█▌▐█▄▪▐█▐█▄▄▌▐█•█▌
 ▀▀▀▀  ▀▀▀ .▀  ▀ ▀█▄▀▪ ▀▀▀▀ ▀▪·▀▀▀▀  ▀▀▀ .▀  ▀
▄▄▄  ▄▄▄ .▄▄▌   ▄▄▄·  ▄· ▄▌▄▄▄ .▄▄▄
▀▄ █·▀▄.▀·██•  ▐█ ▀█ ▐█▪██▌▀▄.▀·▀▄ █·
▐▀▀▄ ▐▀▀▪▄██▪  ▄█▀▀█ ▐█▌▐█▪▐▀▀▪▄▐▀▀▄
▐█•█▌▐█▄▄▌▐█▌▐▌▐█ ▪▐▌ ▐█▀·.▐█▄▄▌▐█•█▌
.▀  ▀ ▀▀▀ .▀▀▀  ▀  ▀   ▀ •  ▀▀▀ .▀  ▀
	`)

	relayer := relay.Relayer{
		Source: relay.RedisSource{
			Endpoint: "localhost:6379",
			Pattern:  "redis-queue*",
		},
		Sink: relay.KafkaSink{
			Brokers: "10.32.230.105:9092,10.33.249.164:9092,10.33.205.205:9092",
		},
		Options: &relay.Options{
			SourceBatchSize: 20,
			SinkBatchSize:   10,
		},
	}

	relayer.Run()

}
