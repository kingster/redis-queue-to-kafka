package main

import (
	"./pkg/relay"
	"flag"
	"fmt"
	"github.com/kingster/go-metrics-statsd"
	"github.com/rcrowley/go-metrics"
	"github.com/tkanos/gonfig"
	"log"
	"net"
	"os"
	"time"
)

type AppConfiguration struct {
	RedisEndpoint   string
	QueuePattern    string
	KafkaBrokers    string
	SourceBatchSize int
	SinkBatchSize   int
	ZKConnect       []string
	Zone            string
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

	go metrics.Log(metrics.DefaultRegistry, 30*time.Second, log.New(os.Stdout, "metrics: ", log.Lmicroseconds))

	addr, _ := net.ResolveUDPAddr("udp", ":8125")
	go statsd.StatsD(metrics.DefaultRegistry, 30*time.Second, "strowger", addr)

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

	leadership := relay.LeaderElector{
		ZKConnect:    configuration.ZKConnect,
		ElectionNode: fmt.Sprintf("/election/relayer-%s", configuration.Zone),
	}

	leadership.SetTask(relayer.Run)
	leadership.Elect()

}
