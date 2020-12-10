package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"log"
	"os"
)



func InitProducer() (sarama.SyncProducer, error) {
	sarama.Logger = log.New(os.Stdout, "", log.Ltime)
	
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

func Produce(message string, headers map[string]string, producer sarama.SyncProducer, topic string) {
	msg := &sarama.ProducerMessage{
		Topic:   topic,
		Value:   sarama.StringEncoder(message),
		Headers: convertHeaders(headers),
	}
	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		fmt.Println("Error publish: ", err.Error())
	}

	fmt.Println("Partition: ", partition)
	fmt.Println("Offset: ", offset)
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
