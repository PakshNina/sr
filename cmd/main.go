package main

import (
	"fmt"
	"log"

	"sr/internal/config"
	"sr/internal/kafka"
	"sr/pkg/test"
)

const (
	topic = "topic.v1"
)

func main() {
	cfg := config.NewConfig()
	producer, err := kafka.NewProducer(cfg.KafkaURL, cfg.SchemaRegistryURL)
	defer producer.Close()

	if err != nil {
		log.Fatalf("error with producer: %v", err)
	}
	testMSG := test.TestMessage{Value: 42}
	offset, err := producer.ProduceMessage(&testMSG, topic)
	if err != nil {
		log.Fatalf("error with produce message: %v", err)
	}
	fmt.Println(offset)
}
