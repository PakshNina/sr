package kafka

import (
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
	Run(messageType protoreflect.MessageType, topic string) error
	Close()
}

type srConsumer struct {
	consumer     *kafka.Consumer
	deserializer *protobuf.Deserializer
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
		return nil, err
	}

	sr, err := schemaregistry.NewClient(schemaregistry.NewConfig(srURL))
	if err != nil {
		return nil, err
	}

	d, err := protobuf.NewDeserializer(sr, serde.ValueSerde, protobuf.NewDeserializerConfig())
	if err != nil {
		return nil, err
	}
	return &srConsumer{
		consumer:     c,
		deserializer: d,
	}, nil
}

// RegisterMessage add simpleHandler and register schema in SR
func (c *srConsumer) RegisterMessage(messageType protoreflect.MessageType) error {
	return nil
}

// Run consumer
func (c *srConsumer) Run(messageType protoreflect.MessageType, topic string) error {
	if err := c.consumer.SubscribeTopics([]string{topic}, nil); err != nil {
		return err
	}
	if err := c.deserializer.ProtoRegistry.RegisterMessage(messageType); err != nil {
		return err
	}
	for {
		kafkaMsg, err := c.consumer.ReadMessage(noTimeout)
		if err != nil {
			return err
		}
		msg, err := c.deserializer.Deserialize(topic, kafkaMsg.Value)
		if err != nil {
			return err
		}
		c.handleMessage(msg, int64(kafkaMsg.TopicPartition.Offset))
		if _, err = c.consumer.CommitMessage(kafkaMsg); err != nil {
			return err
		}
	}
}

func (c *srConsumer) handleMessage(message interface{}, offset int64) {
	fmt.Printf("message %v with offset %d\n", message, offset)
}

// Close all connections
func (c *srConsumer) Close() {
	if err := c.consumer.Close(); err != nil {
		log.Fatal(err)
	}
	c.deserializer.Close()
}
