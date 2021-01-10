package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"log"
	"os"
)

const (
	kafkaConn     = "10.20.30.45:4521"
	topic         = "vnext-datahub-sarama-test"
	topic2        = "vnext-datahub-sarama-test2"
	topic3        = "vnext-datahub-sarama-test3"
	topic4        = "vnext-datahub-sarama-test4"
	consumerGroup = "datahub"
)

func main() {
	sarama.Logger = log.New(os.Stdout, "", log.Ltime)

	//consumer, err := initConsumer()
	//if err != nil {
	//	fmt.Println("Error consumer goup: ", err.Error())
	//	os.Exit(1)
	//}
	//defer func() {
	//	err := consumer.Close()
	//	if err != nil {
	//		fmt.Println(err)
	//	}
	//}()
	//
	//consumeSync(consumer)

	err := initConsumerGroup()
	if err != nil {
		fmt.Println("Error consumer goup: ", err.Error())
		os.Exit(1)
	}
}
