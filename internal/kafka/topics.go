package kafka

const (
	TopicRawSignals      = "raw-signals"
	TopicEnrichedSignals = "enriched-signals"
	TopicDLQIngestion    = "dlq-ingestion"
	TopicDLQEnrichment   = "dlq-enrichment"
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
		DLQIngestionTopic(),
		DLQEnrichmentTopic(),
	}
}

func RawSignalsTopic() TopicConfig {
	return TopicConfig{
		Name:              TopicRawSignals,
		Partitions:        12,
		ReplicationFactor: 1,
		RetentionMs:       7 * 24 * 60 * 60 * 1000,
		Compression:       "lz4",
	}
}

func EnrichedSignalsTopic() TopicConfig {
	return TopicConfig{
		Name:              TopicEnrichedSignals,
		Partitions:        12,
		ReplicationFactor: 1,
		RetentionMs:       30 * 24 * 60 * 60 * 1000,
		Compression:       "lz4",
	}
}

func DLQIngestionTopic() TopicConfig {
	return TopicConfig{
		Name:              TopicDLQIngestion,
		Partitions:        3,
		ReplicationFactor: 1,
		RetentionMs:       90 * 24 * 60 * 60 * 1000,
		Compression:       "gzip",
	}
}

func DLQEnrichmentTopic() TopicConfig {
	return TopicConfig{
		Name:              TopicDLQEnrichment,
		Partitions:        3,
		ReplicationFactor: 1,
		RetentionMs:       90 * 24 * 60 * 60 * 1000,
		Compression:       "gzip",
	}
}
