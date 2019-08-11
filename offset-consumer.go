package main

import (
	"fmt"
	"time"

	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

func main() {

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "staging-kafka-1.svr.tiki.services:9092,staging-kafka-2.svr.tiki.services:9092,staging-kafka-3.svr.tiki.services:9092",
		"group.id":          "source-is-dev-cluster",
		"auto.offset.reset": "earliest",
	})

	if err != nil {
		panic(err)
	}

	c.SubscribeTopics([]string{"__consumer_offsets", "^aRegex.*[Tt]opic"}, nil)

	for {
		time.Sleep(10 * time.Millisecond)
		msg, err := c.ReadMessage(-1)
		if err == nil {
			fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
		} else {
			// The client will automatically try to recover from all errors.
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
		}
	}

	c.Close()
}
