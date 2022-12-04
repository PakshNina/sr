package main

import (
	"fmt"
	"log"

	"sr/internal/config"
	"sr/internal/queue"
	"sr/pkg/test"
)

const (
	topic = "topic.v1"
)

func main() {
	cfg := config.NewConfig()
	producer, err := queue.NewProducerClient(cfg.KafkaURL, cfg.SchemaRegistryURL)
	if err != nil {
		log.Fatalf("error with producer: %v", err)
	}
	testMSG := test.TestMessage{Value: 42}
	offset, err := producer.ProduceMessage(&testMSG, topic)
	if err != nil {
		log.Fatalf("error with produce message: %v", err)
	}
	fmt.Println(offset)
	producer.Close()
}
