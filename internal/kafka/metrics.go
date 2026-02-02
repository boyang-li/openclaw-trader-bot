package kafka

import (
	"github.com/prometheus/client_golang/prometheus"
)

type ProducerMetrics struct {
	MessagesSent prometheus.Counter
	BytesWritten prometheus.Counter
	Errors       prometheus.Counter
	SendLatency  prometheus.Histogram
	BatchSize    prometheus.Histogram
}

func NewProducerMetrics(namespace string) *ProducerMetrics {
	return &ProducerMetrics{
		MessagesSent: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "producer_messages_sent_total",
			Help:      "Total number of messages sent to Kafka",
		}),
		BytesWritten: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "producer_bytes_written_total",
			Help:      "Total bytes written to Kafka",
		}),
		Errors: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "producer_errors_total",
			Help:      "Total number of producer errors",
		}),
		SendLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "producer_send_latency_seconds",
			Help:      "Latency of send operations in seconds",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 10),
		}),
		BatchSize: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "producer_batch_size",
			Help:      "Size of batches sent to Kafka",
			Buckets:   prometheus.LinearBuckets(1, 10, 10),
		}),
	}
}

func (m *ProducerMetrics) Register(reg prometheus.Registerer) error {
	collectors := []prometheus.Collector{
		m.MessagesSent,
		m.BytesWritten,
		m.Errors,
		m.SendLatency,
		m.BatchSize,
	}

	for _, c := range collectors {
		if err := reg.Register(c); err != nil {
			return err
		}
	}
	return nil
}
