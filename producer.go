package main

import (
	"fmt"
	"time"

	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

func main() {

	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "dev-kafka-1.svr.tiki.services:9092", "acks": "all"})
	if err != nil {
		panic(err)
	}

	defer p.Close()

	// Delivery report handler for produced messages
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
				} //else {
				//fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
				//}
			}
		}
	}()

	// Produce messages to topic (asynchronously)
	topic := "poc_replicator_topic"
	fmt.Println("Start sending data to topic %v", topic)
	for i := 0; i < 100000; i++ {
		time.Sleep(10 * time.Millisecond)
		word := fmt.Sprintf("Test kafka, test replicator Test kafka, test replicator Test kafka, test replicatorTest kafka, test replicatorTest kafka, test replicatorTest kafka, test replicatorTest kafka, test replicatorTest kafka, test replicatorTest kafka, test replicatorTest kafka, test replicatorTest kafka, test replicatorTest kafka, test replicatorTest kafka, test replicatorTest kafka, test replicator Test kafka, test replicator %d", i)
		//fmt.Println("Data: ", word)
		p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          []byte(word),
		}, nil)
	}

	// Wait for message deliveries before shutting down
	p.Flush(5 * 1000)
}
