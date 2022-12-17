package main

import (
	"context"
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
	handler := kafka.NewSimpleHandler(topic, (&test.TestMessage{}).ProtoReflect().Type())
	if err := consumer.AddHandler(handler); err != nil {
		log.Fatal(err)
	}
	ctx := context.Background()
	if err := consumer.Run(ctx); err != nil {
		log.Fatal(err)
	}
}
