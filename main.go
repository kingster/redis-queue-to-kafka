package main

import (
	"fmt"
	"./pkg/relay"
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
			Pattern: "redis-queue*",
		},
		Sink: relay.KafkaSink{
			Brokers: "",
		},
		Options: &relay.Options{
			SourceBatchSize: 20,
			SinkBatchSize: 10,
		},
	}

	relayer.Run()

}
