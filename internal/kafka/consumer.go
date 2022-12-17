package kafka

import (
	"context"
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/confluentinc/confluent-kafka-go/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/schemaregistry/serde/protobuf"
	"google.golang.org/protobuf/reflect/protoreflect"
)

const (
	consumerGroupID       = "test-consumer"
	defaultSessionTimeout = 6000
	noTimeout             = -1
)

// SRConsumer interface
type SRConsumer interface {
	AddHandler(handler Handler) error
	Run(ctx context.Context) error
	Close()
}

type srConsumer struct {
	consumer     *kafka.Consumer
	deserializer *protobuf.Deserializer
	handler      Handler
}

// Handler interface
type Handler interface {
	HandleMessage(ctx context.Context, message interface{}, offset int64) error
	TopicName() string
	MessageType() protoreflect.MessageType
}

// NewConsumer returns new consumer with schema registry
func NewConsumer(kafkaURL, srURL string) (SRConsumer, error) {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  kafkaURL,
		"group.id":           consumerGroupID,
		"session.timeout.ms": defaultSessionTimeout,
		"enable.auto.commit": false,
	})
	if err != nil {
		return nil, fmt.Errorf("error with consumer: %w", err)
	}

	sr, err := schemaregistry.NewClient(schemaregistry.NewConfig(srURL))
	if err != nil {
		return nil, fmt.Errorf("error with schema registry: %w", err)
	}

	d, err := protobuf.NewDeserializer(sr, serde.ValueSerde, protobuf.NewDeserializerConfig())
	if err != nil {
		return nil, fmt.Errorf("error with deserializer: %w", err)
	}
	return &srConsumer{
		consumer:     c,
		deserializer: d,
	}, nil
}

// AddHandler add simpleHandler and register schema in SR
func (c *srConsumer) AddHandler(handler Handler) error {
	c.handler = handler
	if err := c.deserializer.ProtoRegistry.RegisterMessage(handler.MessageType()); err != nil {
		return fmt.Errorf("error with RegisterMessage: %w", err)
	}
	return nil
}

// Run consumer
func (c *srConsumer) Run(ctx context.Context) error {
	if err := c.consumer.SubscribeTopics([]string{c.handler.TopicName()}, nil); err != nil {
		return err
	}
	for {
		kafkaMsg, err := c.consumer.ReadMessage(noTimeout)
		if err != nil {
			return err
		}
		msg, err := c.deserializer.Deserialize(c.handler.TopicName(), kafkaMsg.Value)
		if err = c.handler.HandleMessage(ctx, msg, int64(kafkaMsg.TopicPartition.Offset)); err != nil {
			continue
		}
		if _, err = c.consumer.CommitMessage(kafkaMsg); err != nil {
			return err
		}
	}
}

// Close all connections
func (c *srConsumer) Close() {
	if err := c.consumer.Close(); err != nil {
		log.Fatal(err)
	}
	c.deserializer.Close()
}
