package main

import (
	"fmt"
	"github.com/Shopify/sarama"
)

//
//import (
//	"fmt"
//	"github.com/Shopify/sarama"
//	"log"
//	"os"
//	"time"
//)
//
//const (
//	zookeeperConn = "10.4.1.29:2181"
//	cgroup = "zgroup"
//	topic = "senz"
//)
//
func initConsumer() (sarama.Consumer, error) {
	config := sarama.NewConfig()
	config.Producer.Retry.Max = 5
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true

	consumer, err := sarama.NewConsumer([]string{kafkaConn}, config)
	if err != nil {
		return nil, err
	}

	topics, err := consumer.Topics()
	if err != nil {
		return nil, err
	}
	fmt.Println(topics)

	partitions, err := consumer.Partitions(topic)
	if err != nil {
		return nil, err
	}
	fmt.Println(partitions)

	return consumer, nil
}

func consumeSync(partitionConsumer sarama.PartitionConsumer) {
	for msg := range partitionConsumer.Messages() {
		fmt.Printf("Offset:\t%d\n", msg.Offset)
		fmt.Printf("Key:\t%s\n", string(msg.Key))
		fmt.Printf("Value:\t%s\n", string(msg.Value))
		for _, header := range msg.Headers {
			fmt.Printf("HeaderKey:\t%s ", string(header.Key))
			fmt.Printf("HeaderValue:\t%s\n ", string(header.Value))
		}
		fmt.Println()
	}

	if err := partitionConsumer.Close(); err != nil {
		fmt.Println("Failed to close consumer: ", err)
	}
}

func consume(partitionConsumer sarama.PartitionConsumer) {
	//var (
	//	bufferSize = flag.Int("buffer-size", 256, "The buffer size of the message channel.")
	//	messages = make(chan *sarama.ConsumerMessage, *bufferSize)
	//	closing  = make(chan struct{})
	//	wg       sync.WaitGroup
	//)
	//
	//go func() {
	//	signals := make(chan os.Signal, 1)
	//	signal.Notify(signals, syscall.SIGTERM, os.Interrupt)
	//	<-signals
	//	fmt.Println("Initiating shutdown of consumer...")
	//	close(closing)
	//}()
	//
	//go func(partitionConsumer sarama.PartitionConsumer) {
	//	<-closing
	//	partitionConsumer.AsyncClose()
	//}(partitionConsumer)
	//
	//wg.Add(1)
	//go func(partitionConsumer sarama.PartitionConsumer) {
	//	defer wg.Done()
	//	for message := range partitionConsumer.Messages() {
	//		messages <- message
	//	}
	//}(partitionConsumer)
	//
	//go func() {
	//	for msg := range messages {
	//		fmt.Printf("Partition:\t%d\n", msg.Partition)
	//		fmt.Printf("Offset:\t%d\n", msg.Offset)
	//		fmt.Printf("Key:\t%s\n", string(msg.Key))
	//		fmt.Printf("Value:\t%s\n", string(msg.Value))
	//		fmt.Println()
	//	}
	//}()
	//
	//wg.Wait()
	//fmt.Println("Done consuming topic")
	//close(messages)

	if err := partitionConsumer.Close(); err != nil {
		fmt.Println("Failed to close consumer: ", err)
	}
}

//func getPartitions(c sarama.Consumer) ([]int32, error) {
//	tmp := strings.Split(*partitions, ",")
//	var pList []int32
//	for i := range tmp {
//		val, err := strconv.ParseInt(tmp[i], 10, 32)
//		if err != nil {
//			return nil, err
//		}
//		pList = append(pList, int32(val))
//	}
//
//	return pList, nil
//}
