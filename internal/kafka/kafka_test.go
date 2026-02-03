package kafka

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTopicConstants(t *testing.T) {
	assert.Equal(t, "l1.signals.raw", TopicRawSignals)
	assert.Equal(t, "l1.signals.enriched", TopicEnrichedSignals)
	assert.Equal(t, "l1.signals.filtered", TopicFilteredSignals)
	assert.Equal(t, "l1.signals.dlq", TopicDLQ)
}

func TestDefaultTopics(t *testing.T) {
	topics := DefaultTopics()

	assert.Len(t, topics, 4)

	topicNames := make([]string, len(topics))
	for i, topic := range topics {
		topicNames[i] = topic.Name
	}

	assert.Contains(t, topicNames, TopicRawSignals)
	assert.Contains(t, topicNames, TopicEnrichedSignals)
	assert.Contains(t, topicNames, TopicFilteredSignals)
	assert.Contains(t, topicNames, TopicDLQ)
}

func TestRawSignalsTopic(t *testing.T) {
	topic := RawSignalsTopic()

	assert.Equal(t, TopicRawSignals, topic.Name)
	assert.Equal(t, 6, topic.Partitions)
	assert.Equal(t, 1, topic.ReplicationFactor)
	assert.Equal(t, int64(7*24*60*60*1000), topic.RetentionMs)
	assert.Equal(t, "lz4", topic.Compression)
}

func TestEnrichedSignalsTopic(t *testing.T) {
	topic := EnrichedSignalsTopic()

	assert.Equal(t, TopicEnrichedSignals, topic.Name)
	assert.Equal(t, 6, topic.Partitions)
	assert.Equal(t, 1, topic.ReplicationFactor)
	assert.Equal(t, int64(7*24*60*60*1000), topic.RetentionMs)
	assert.Equal(t, "lz4", topic.Compression)
}

func TestFilteredSignalsTopic(t *testing.T) {
	topic := FilteredSignalsTopic()

	assert.Equal(t, TopicFilteredSignals, topic.Name)
	assert.Equal(t, 3, topic.Partitions)
	assert.Equal(t, 1, topic.ReplicationFactor)
	assert.Equal(t, int64(30*24*60*60*1000), topic.RetentionMs)
	assert.Equal(t, "lz4", topic.Compression)
}

func TestDLQTopic(t *testing.T) {
	topic := DLQTopic()

	assert.Equal(t, TopicDLQ, topic.Name)
	assert.Equal(t, 1, topic.Partitions)
	assert.Equal(t, 1, topic.ReplicationFactor)
	assert.Equal(t, int64(30*24*60*60*1000), topic.RetentionMs)
	assert.Equal(t, "gzip", topic.Compression)
}

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	assert.Equal(t, []string{"localhost:9092"}, cfg.Brokers)
	assert.Equal(t, TopicRawSignals, cfg.Topic)
	assert.Equal(t, 100, cfg.BatchSize)
	assert.Equal(t, 100*time.Millisecond, cfg.BatchTimeout)
	assert.Equal(t, 3, cfg.MaxAttempts)
	assert.Equal(t, 10*time.Second, cfg.WriteTimeout)
	assert.Equal(t, kafka.RequireAll, cfg.RequiredAcks)
	assert.Equal(t, kafka.Lz4, cfg.Compression)
	assert.False(t, cfg.Async)
	assert.Equal(t, 1000, cfg.QueueCapacity)
}

func TestConfig_Validate_Valid(t *testing.T) {
	cfg := DefaultConfig()
	err := cfg.Validate()
	assert.NoError(t, err)
}

func TestConfig_Validate_NoBrokers(t *testing.T) {
	cfg := Config{
		Brokers:   []string{},
		Topic:     "test-topic",
		BatchSize: 100,
	}

	err := cfg.Validate()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "brokers required")
}

func TestConfig_Validate_NoTopic(t *testing.T) {
	cfg := Config{
		Brokers:   []string{"localhost:9092"},
		Topic:     "",
		BatchSize: 100,
	}

	err := cfg.Validate()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "topic required")
}

func TestConfig_Validate_InvalidBatchSize(t *testing.T) {
	tests := []struct {
		name      string
		batchSize int
	}{
		{"zero", 0},
		{"negative", -1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Config{
				Brokers:   []string{"localhost:9092"},
				Topic:     "test-topic",
				BatchSize: tt.batchSize,
			}

			err := cfg.Validate()
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "batch size must be positive")
		})
	}
}

func TestConfigFromEnv_Defaults(t *testing.T) {
	cfg, err := ConfigFromEnv()
	require.NoError(t, err)

	assert.Equal(t, []string{"localhost:9092"}, cfg.Brokers)
	assert.Equal(t, TopicRawSignals, cfg.Topic)
}

func TestConfigFromEnv_EnvOverrides(t *testing.T) {
	t.Setenv("KAFKA_BROKERS", "broker1:9092,broker2:9092")
	t.Setenv("KAFKA_TOPIC", "custom-topic")

	cfg, err := ConfigFromEnv()
	require.NoError(t, err)

	assert.Equal(t, []string{"broker1:9092", "broker2:9092"}, cfg.Brokers)
	assert.Equal(t, "custom-topic", cfg.Topic)
}

func TestConfigFromEnv_SecuritySettings(t *testing.T) {
	t.Setenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
	t.Setenv("KAFKA_SASL_MECHANISM", "PLAIN")
	t.Setenv("KAFKA_SASL_USERNAME", "user")
	t.Setenv("KAFKA_SASL_PASSWORD", "pass")

	cfg, err := ConfigFromEnv()
	require.NoError(t, err)

	assert.Equal(t, "SASL_SSL", cfg.SecurityProtocol)
	assert.Equal(t, "PLAIN", cfg.SASLMechanism)
	assert.Equal(t, "user", cfg.SASLUsername)
	assert.Equal(t, "pass", cfg.SASLPassword)
}

func TestNewProducerMetrics(t *testing.T) {
	m := NewProducerMetrics("test")

	assert.NotNil(t, m.MessagesSent)
	assert.NotNil(t, m.BytesWritten)
	assert.NotNil(t, m.Errors)
	assert.NotNil(t, m.SendLatency)
	assert.NotNil(t, m.BatchSize)
}

func TestProducerMetrics_Register(t *testing.T) {
	m := NewProducerMetrics("test_register")
	reg := prometheus.NewRegistry()

	err := m.Register(reg)
	assert.NoError(t, err)

	families, err := reg.Gather()
	require.NoError(t, err)

	metricNames := make([]string, len(families))
	for i, f := range families {
		metricNames[i] = *f.Name
	}

	assert.Contains(t, metricNames, "test_register_producer_messages_sent_total")
	assert.Contains(t, metricNames, "test_register_producer_bytes_written_total")
	assert.Contains(t, metricNames, "test_register_producer_errors_total")
	assert.Contains(t, metricNames, "test_register_producer_send_latency_seconds")
	assert.Contains(t, metricNames, "test_register_producer_batch_size")
}

func TestProducerMetrics_Register_Duplicate(t *testing.T) {
	m := NewProducerMetrics("test_dup")
	reg := prometheus.NewRegistry()

	err := m.Register(reg)
	require.NoError(t, err)

	m2 := NewProducerMetrics("test_dup")
	err = m2.Register(reg)
	assert.Error(t, err)
}
