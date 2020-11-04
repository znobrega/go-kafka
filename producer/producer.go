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

func InitProducer() (sarama.SyncProducer, error) {
	// setup sarama log to stdout
	sarama.Logger = log.New(os.Stdout, "", log.Ltime)

	// producer config
	config := sarama.NewConfig()
	config.Producer.Retry.Max = 5
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true

	// async producer
	//prd, err := sarama.NewAsyncProducer([]string{kafkaConn}, config)

	// sync producer
	prd, err := sarama.NewSyncProducer([]string{kafkaConn}, config)

	return prd, err
}

func Produce(message string, headers map[string]string, producer sarama.SyncProducer) {
	// publish sync
	msg := &sarama.ProducerMessage{
		Topic:   topic,
		Value:   sarama.StringEncoder(message),
		Headers: convertHeaders(headers),
	}
	p, o, err := producer.SendMessage(msg)
	if err != nil {
		fmt.Println("Error publish: ", err.Error())
	}

	// publish async
	//producer.Input() <- &sarama.ProducerMessage{

	fmt.Println("Partition: ", p)
	fmt.Println("Offset: ", o)
}

func convertHeaders(headers map[string]string) []sarama.RecordHeader {
	output := make([]sarama.RecordHeader, 0)
	for key, value := range headers {
		output = append(output, sarama.RecordHeader{
			Key:   []byte(key),
			Value: []byte(value),
		})
	}
	return output
}
