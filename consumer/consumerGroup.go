package main

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"time"
)

type ConsumerGroup interface {
	Consume()
}

type KafkaConsumerGroup struct {
	consumerGroup   sarama.ConsumerGroup
	consumerHandler sarama.ConsumerGroupHandler
}

func NewKafkaConsumerGroup(consumerGroup sarama.ConsumerGroup, consumerHandler sarama.ConsumerGroupHandler) ConsumerGroup {
	return KafkaConsumerGroup{
		consumerGroup:   consumerGroup,
		consumerHandler: consumerHandler,
	}
}

func initConsumerGroup() error {
	config := sarama.NewConfig()
	config.Producer.Retry.Max = 5
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true

	consumerGroup, err := sarama.NewConsumerGroup([]string{kafkaConn}, consumerGroup, config)
	if err != nil {
		return err
	}

	consumerGroupHandler := ConsumerHandler{}

	go func() {
		for err := range consumerGroup.Errors() {
			panic(err)
		}
	}()

	kafkaConsumerGroup := NewKafkaConsumerGroup(consumerGroup, consumerGroupHandler)

	kafkaConsumerGroup.Consume()

	return nil
}

func (c KafkaConsumerGroup) Consume() {
	ctx := context.Background()
	for {
		topics := []string{topic, topic2, topic3, topic4}
		err := c.consumerGroup.Consume(ctx, topics, c.consumerHandler)
		if err != nil {
			fmt.Printf("kafka consume failed: %v, sleeping and retry in a moment\n", err)
			time.Sleep(time.Second)
		}
	}
}

type ConsumerHandler struct{}

func (c ConsumerHandler) Setup(session sarama.ConsumerGroupSession) error {
	fmt.Println("SETUP CONSUMER GROUP")
	return nil
}

func (c ConsumerHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	fmt.Println("CLEANUP CONSUMER GROUP")
	return nil
}

func (c ConsumerHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	fmt.Println("CONSUMERCLAIM CONSUMER GROUP")
	for msg := range claim.Messages() {
		fmt.Printf("Offset:\t%d\n", msg.Offset)
		fmt.Printf("Key:\t%s\n", string(msg.Key))
		fmt.Printf("Topic:\t%s\n", msg.Topic)
		fmt.Printf("Partition:\t%d\n", msg.Partition)
		fmt.Printf("Value:\t%s\n", string(msg.Value))
		for _, header := range msg.Headers {
			fmt.Printf("HeaderKey:\t%s ", string(header.Key))
			fmt.Printf("HeaderValue:\t%s\n ", string(header.Value))
		}
		fmt.Println()
		// return fmt.Errorf("FORCING ERROR")

		session.MarkMessage(msg, "")
	}
	return nil
}
