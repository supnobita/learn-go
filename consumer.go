package main

import (
	"fmt"
	"time"

	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

func main() {

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "dev-kafka-1.svr.tiki.services:9092,dev-kafka-2.svr.tiki.services:9092,dev-kafka-3.svr.tiki.services:9092",
		"group.id":          "poc_kafka_replicator-source-is-dev-cluster",
		"auto.offset.reset": "earliest",
	})

	if err != nil {
		panic(err)
	}

	c.SubscribeTopics([]string{"poc_replicator_topic", "^aRegex.*[Tt]opic"}, nil)

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
