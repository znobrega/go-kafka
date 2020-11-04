package main

import (
	"bufio"
	"fmt"
	"os"
)

func main() {
	// create producer
	producer, err := InitProducer()
	if err != nil {
		fmt.Println("Error producer: ", err.Error())
		os.Exit(1)
	}

	// read command line input
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print("Enter msg: ")
		msg, _ := reader.ReadString('\n')

		headers := make(map[string]string, 0)
		headers["header_1"] = "content_1"
		headers["carlos"] = "test"

		Produce(msg, headers, producer)

		// publish with go routine
		// go publish(msg, producer)
	}
}
