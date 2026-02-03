package kafka

import (
	"errors"
	"os"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

type Config struct {
	Brokers          []string
	Topic            string
	SecurityProtocol string
	SASLMechanism    string
	SASLUsername     string
	SASLPassword     string
	BatchSize        int
	BatchTimeout     time.Duration
	MaxAttempts      int
	WriteTimeout     time.Duration
	RequiredAcks     kafka.RequiredAcks
	Compression      kafka.Compression
	Async            bool
	QueueCapacity    int
}

func DefaultConfig() Config {
	return Config{
		Brokers:       []string{"localhost:9092"},
		Topic:         TopicRawSignals,
		BatchSize:     100,
		BatchTimeout:  100 * time.Millisecond,
		MaxAttempts:   3,
		WriteTimeout:  10 * time.Second,
		RequiredAcks:  kafka.RequireAll,
		Compression:   kafka.Lz4,
		Async:         false,
		QueueCapacity: 1000,
	}
}

func (c *Config) Validate() error {
	if len(c.Brokers) == 0 {
		return errors.New("kafka: brokers required")
	}
	if c.Topic == "" {
		return errors.New("kafka: topic required")
	}
	if c.BatchSize <= 0 {
		return errors.New("kafka: batch size must be positive")
	}
	return nil
}

func ConfigFromEnv() (*Config, error) {
	cfg := DefaultConfig()

	if brokers := os.Getenv("KAFKA_BROKERS"); brokers != "" {
		cfg.Brokers = strings.Split(brokers, ",")
	}
	if topic := os.Getenv("KAFKA_TOPIC"); topic != "" {
		cfg.Topic = topic
	}
	cfg.SecurityProtocol = os.Getenv("KAFKA_SECURITY_PROTOCOL")
	cfg.SASLMechanism = os.Getenv("KAFKA_SASL_MECHANISM")
	cfg.SASLUsername = os.Getenv("KAFKA_SASL_USERNAME")
	cfg.SASLPassword = os.Getenv("KAFKA_SASL_PASSWORD")

	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}
