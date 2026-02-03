package kafka

const (
	TopicRawSignals      = "l1.signals.raw"
	TopicEnrichedSignals = "l1.signals.enriched"
	TopicFilteredSignals = "l1.signals.filtered"
	TopicDLQ             = "l1.signals.dlq"
)

type TopicConfig struct {
	Name              string
	Partitions        int
	ReplicationFactor int
	RetentionMs       int64
	Compression       string
}

func DefaultTopics() []TopicConfig {
	return []TopicConfig{
		RawSignalsTopic(),
		EnrichedSignalsTopic(),
		FilteredSignalsTopic(),
		DLQTopic(),
	}
}

func RawSignalsTopic() TopicConfig {
	return TopicConfig{
		Name:              TopicRawSignals,
		Partitions:        6,
		ReplicationFactor: 1,
		RetentionMs:       7 * 24 * 60 * 60 * 1000,
		Compression:       "lz4",
	}
}

func EnrichedSignalsTopic() TopicConfig {
	return TopicConfig{
		Name:              TopicEnrichedSignals,
		Partitions:        6,
		ReplicationFactor: 1,
		RetentionMs:       7 * 24 * 60 * 60 * 1000,
		Compression:       "lz4",
	}
}

func FilteredSignalsTopic() TopicConfig {
	return TopicConfig{
		Name:              TopicFilteredSignals,
		Partitions:        3,
		ReplicationFactor: 1,
		RetentionMs:       30 * 24 * 60 * 60 * 1000,
		Compression:       "lz4",
	}
}

func DLQTopic() TopicConfig {
	return TopicConfig{
		Name:              TopicDLQ,
		Partitions:        1,
		ReplicationFactor: 1,
		RetentionMs:       30 * 24 * 60 * 60 * 1000,
		Compression:       "gzip",
	}
}
