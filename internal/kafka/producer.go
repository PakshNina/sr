package kafka

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/confluentinc/confluent-kafka-go/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/schemaregistry/serde/protobuf"
	"google.golang.org/protobuf/proto"
)

const (
	nullOffset = -1
)

type SRProducer interface {
	ProduceMessage(msg proto.Message, topic string) (int64, error)
	Close()
}

type srProducer struct {
	producer   *kafka.Producer
	serializer serde.Serializer
}

// NewProducer returns kafka producer with schema registry
func NewProducer(kafkaURL, srURL string) (SRProducer, error) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": kafkaURL})
	if err != nil {
		return nil, fmt.Errorf("error with creating producer: %w", err)
	}
	c, err := schemaregistry.NewClient(schemaregistry.NewConfig(srURL))
	if err != nil {
		return nil, fmt.Errorf("error with creating schema registry client: %w", err)
	}
	s, err := protobuf.NewSerializer(c, serde.ValueSerde, protobuf.NewSerializerConfig())
	if err != nil {
		return nil, fmt.Errorf("error with creating serializer: %w", err)
	}
	return &srProducer{
		producer:   p,
		serializer: s,
	}, nil
}

// ProduceMessage sends serialized message to kafka using schema registry
func (p *srProducer) ProduceMessage(msg proto.Message, topic string) (int64, error) {
	kafkaChan := make(chan kafka.Event)
	defer close(kafkaChan)
	payload, err := p.serializer.Serialize(topic, msg)
	if err != nil {
		return nullOffset, fmt.Errorf("error with serializing message: %w", err)
	}
	if err = p.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic},
		Value:          payload,
	}, kafkaChan); err != nil {
		return nullOffset, fmt.Errorf("error with producing message: %w", err)
	}
	e := <-kafkaChan
	switch ev := e.(type) {
	case *kafka.Message:
		return int64(ev.TopicPartition.Offset), nil
	case kafka.Error:
		return nullOffset, fmt.Errorf("kafka error: %w", ev)
	}
	return nullOffset, nil
}

// Close schema registry and Kafka
func (p *srProducer) Close() {
	p.serializer.Close()
	p.producer.Close()
}
