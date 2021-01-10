package main

import (
	"bufio"
	"fmt"
	"os"
)

const (
	kafkaConn = "10.20.30.45:4521"
	topic     = "vnext-datahub-sarama-test"
	topic2    = "vnext-datahub-sarama-test2"
	topic3    = "vnext-datahub-sarama-test3"
	topic4    = "vnext-datahub-sarama-test4"
)

func main() {
	// create producer
	producer, err := InitProducer()
	if err != nil {
		fmt.Println("Error producer: ", err.Error())
		os.Exit(1)
	}

	kafkaProducer := NewKafkaProducer(producer)

	// read command line input
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print("Enter msg: ")
		msg, _ := reader.ReadString('\n')

		headers := make(map[string]string, 0)
		headers["carlos1"] = "content"
		headers["carlos2"] = "test"

		kafkaProducer.Produce("PRODUCER 1 "+msg, headers, topic)
		kafkaProducer.Produce("PRODUCER 2 "+msg, headers, topic2)
		kafkaProducer.Produce("PRODUCER 3 "+msg, headers, topic3)
		kafkaProducer.Produce("PRODUCER 4 "+msg, headers, topic4)

		// publish with go routine
		// go publish(msg, producer)
	}
}
