package main

import (
	"./pkg/relay"
	"flag"
	"fmt"
	"github.com/tkanos/gonfig"
)

type AppConfiguration struct {
	RedisEndpoint   string
	QueuePattern    string
	KafkaBrokers    string
	SourceBatchSize int
	SinkBatchSize   int
}

func main() {

	configPtr := flag.String("config", "/etc/relayer/relayer.json", "Config File Path ( default /etc/relayer/relayer.json)")
	flag.Parse()

	configuration := AppConfiguration{}
	err := gonfig.GetConf(*configPtr, &configuration)

	if err != nil {
		panic(err)
	}

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
			Endpoint: configuration.RedisEndpoint,
			Pattern:  configuration.QueuePattern,
		},
		Sink: relay.KafkaSink{
			Brokers: configuration.KafkaBrokers,
		},
		Options: &relay.Options{
			SourceBatchSize: configuration.SourceBatchSize,
			SinkBatchSize:   configuration.SinkBatchSize,
		},
	}

	relayer.Run()

}
