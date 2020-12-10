package main

import (
	"context"
	"github.com/Shopify/sarama"
)

type Consumer2 interface{
	Consume()
}

type kafkaConsumer struct {
	consumer sarama.ConsumerGroup
	topics 	[]string
	handler *sarama.ConsumerGroupHandler
}

func (k kafkaConsumer) Consume() {
	k.consumer.Consume(context.Background(), k.topics, *k.handler)
}

type kafkaBuilder struct {
	configs *sarama.Config
}

func (k *kafkaBuilder) NewConsumer() Consumer2 {
	consumerGroup, err := sarama.NewConsumerGroup([]string{kafkaConn}, consumerGroup, k.configs)
	if err != nil {
		return nil
	}

	return kafkaConsumer{
		consumer: consumerGroup,
	}
}

