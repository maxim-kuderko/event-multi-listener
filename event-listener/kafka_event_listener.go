package event_listener

import (
	"encoding/json"
	"log"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/SpotIM/event-multi-listener/types"
)

const KAFKA_CONFIG = "kafka_config"
const CONNECTION_STRING = "connection_string"
const TOPICS = "topics"

type KafkaEventListener struct {
	config *sarama.Config
}

func (KafkaEventListener) Make(config *sarama.Config) EventListener {
	return &KafkaEventListener{
		config: config,
	}
}

func (l *KafkaEventListener) Listen(config ListenerConfig ,topicsOutChannels map[string]chan *types.Event,errors chan error) {
	addrs := strings.Split(config[CONNECTION_STRING].(string), ",")
	kafkaConfig := config[KAFKA_CONFIG].(*sarama.Config)
	consumer, err := sarama.NewConsumer(addrs, kafkaConfig)
	if err != nil {
		log.Panic(err)
	}

	topics := config[TOPICS].([]string)

	topicsOutChannels = make(map[string]chan *types.Event,len(topics))
	errors = make(chan error)

	for _, t := range topics {
		newEventCh := make(chan *types.Event)
		topicsOutChannels[t] = newEventCh
	}

	go consume(topicsOutChannels,errors,consumer)
	return
}

func processMessage(out chan *types.Event,m *sarama.ConsumerMessage){
	var event types.Event
	err := json.Unmarshal(m.Value,&event)
	if err !=nil{
		log.Panic(err)
	}

	out <- &event
}

func processMessages(consumer sarama.PartitionConsumer,out chan *types.Event)  {
	for m := range consumer.Messages() {
		{
			go processMessage(out, m)
		}
	}
}

func processErrors(consumer sarama.PartitionConsumer,out chan error){
	defer close(out)

	for e := range consumer.Errors(){
		out <- e
	}
}

func consumeTopic (out chan *types.Event,errors chan error ,consumer sarama.Consumer,topic string) {
	defer func() {
		close(out)
	}()

	partitions, err := consumer.Partitions(topic)
	if err != nil {
		log.Panic(err)
	}

	for _, p := range partitions {
		pc, err := consumer.ConsumePartition(topic, p, sarama.OffsetNewest)
		if err != nil {
			log.Panic(err)
		}

		go processErrors(pc,errors)
		go processMessages(pc,out)
	}
}

func consume(out map[string]chan *types.Event,errors chan error ,consumer sarama.Consumer) {

	topics, err := consumer.Topics()
	if err != nil {
		log.Panic(err)
	}

	for _, t := range topics {
		consumeTopic(out[t],errors ,consumer,t)
	}
}