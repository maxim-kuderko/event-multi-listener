package partition_consumer

import (
	"github.com/Shopify/sarama"
	"github.com/SpotIM/event-multi-listener/event-listener"
	"github.com/SpotIM/event-multi-listener/types"
	"log"
)

func main() {

	conStr := "127.0.0.1,127.0.0.1"
	kafkaCfg := sarama.NewConfig()
	topics := []string{"test1","tesst2"}
	el := event_listener.KafkaEventListener{}.Make(kafkaCfg)

	listenerConfig := make(map[string]interface{})
	listenerConfig[event_listener.KAFKA_CONFIG] = kafkaCfg
	listenerConfig[event_listener.CONNECTION_STRING] = conStr
	listenerConfig[event_listener.TOPICS] = topics

	topicsOutChannels := make(map[string]chan *types.Event)
	errOut :=make( chan error)

	go el.Listen(listenerConfig,topicsOutChannels,errOut)

	go processError(errOut)

	for t,c := range topicsOutChannels{
		go processTopic(t,c)
	}

	<- make(chan bool)
}

func processError(errors chan error){
	for err := range errors{
		log.Println(err)
	}
}

func processTopic(topic string,events chan *types.Event){
	for e := range events{
		log.Println("this is a new event, event id:",e.Id)
	}
}