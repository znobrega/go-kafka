package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"log"
	"os"
)

const (
	kafkaConn = "10.70.65.212:9094"
	topic     = "vnext-datahub-sarama-test"
)

func main() {
	// setup sarama log to stdout
	sarama.Logger = log.New(os.Stdout, "", log.Ltime)

	// init consumer
	consumer, err := initConsumer()
	if err != nil {
		fmt.Println("Error consumer goup: ", err.Error())
		os.Exit(1)
	}
	defer func() {
		err := consumer.Close()
		if err != nil {
			fmt.Println(err)
		}
	}()

	partitionConsumer, err := consumer.ConsumePartition(topic, 0, 572)
	if err != nil {
		fmt.Println(69, "Failed to start partition consumer: %s", err)
	}
	consumeSync(partitionConsumer)
}
