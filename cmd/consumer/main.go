package main

import (
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
	consumer, err := kafka.NewConsumer(cfg.KafkaURL, cfg.SchemaRegistryURL)
	defer consumer.Close()

	if err != nil {
		log.Fatal(err)
	}
	messageType := (&test.TestMessage{}).ProtoReflect().Type()
	if err := consumer.Run(messageType, topic); err != nil {
		log.Fatal(err)
	}
}
