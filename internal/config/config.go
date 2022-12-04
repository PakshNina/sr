package config

import "os"

// cfg and schema registry cfg
type cfg struct {
	KafkaURL          string
	SchemaRegistryURL string
}

func NewConfig() *cfg {
	return &cfg{
		KafkaURL:          os.Getenv("KAFKA_URL"),
		SchemaRegistryURL: os.Getenv("SCHEMA_REGISTRY_URL"),
	}
}
