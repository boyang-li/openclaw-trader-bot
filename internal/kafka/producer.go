package kafka

import (
	"context"
	"sync"
	"time"

	"github.com/openclaworg/l1-ingestion/internal/signal"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

type ProducerStats struct {
	MessagesSent int64
	BytesWritten int64
	Errors       int64
}

type Producer struct {
	writer  *kafka.Writer
	config  Config
	logger  *zap.Logger
	metrics *ProducerMetrics

	mu           sync.RWMutex
	messagesSent int64
	bytesWritten int64
	errors       int64
}

func NewProducer(config Config, logger *zap.Logger) (*Producer, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}

	if logger == nil {
		logger = zap.NewNop()
	}

	writer := &kafka.Writer{
		Addr:         kafka.TCP(config.Brokers...),
		Topic:        config.Topic,
		Balancer:     &kafka.Hash{},
		BatchSize:    config.BatchSize,
		BatchTimeout: config.BatchTimeout,
		MaxAttempts:  config.MaxAttempts,
		WriteTimeout: config.WriteTimeout,
		RequiredAcks: config.RequiredAcks,
		Compression:  config.Compression,
		Async:        config.Async,
	}

	return &Producer{
		writer:  writer,
		config:  config,
		logger:  logger,
		metrics: NewProducerMetrics("l1_kafka"),
	}, nil
}

func (p *Producer) Send(ctx context.Context, sig signal.Signal) error {
	msg, err := p.signalToMessage(sig)
	if err != nil {
		return err
	}

	start := time.Now()
	err = p.writer.WriteMessages(ctx, msg)
	duration := time.Since(start)

	if err != nil {
		p.mu.Lock()
		p.errors++
		p.mu.Unlock()
		p.logger.Error("failed to send message",
			zap.Error(err),
			zap.String("key", string(msg.Key)),
			zap.Duration("duration", duration))
		return err
	}

	p.mu.Lock()
	p.messagesSent++
	p.bytesWritten += int64(len(msg.Value))
	p.mu.Unlock()

	p.logger.Debug("message sent",
		zap.String("key", string(msg.Key)),
		zap.Int("size", len(msg.Value)),
		zap.Duration("duration", duration))

	return nil
}

func (p *Producer) SendBatch(ctx context.Context, signals []signal.Signal) error {
	messages := make([]kafka.Message, 0, len(signals))

	for _, sig := range signals {
		msg, err := p.signalToMessage(sig)
		if err != nil {
			p.logger.Warn("skipping invalid signal", zap.Error(err))
			continue
		}
		messages = append(messages, msg)
	}

	if len(messages) == 0 {
		return nil
	}

	start := time.Now()
	err := p.writer.WriteMessages(ctx, messages...)
	duration := time.Since(start)

	if err != nil {
		p.mu.Lock()
		p.errors++
		p.mu.Unlock()
		p.logger.Error("failed to send batch",
			zap.Error(err),
			zap.Int("count", len(messages)),
			zap.Duration("duration", duration))
		return err
	}

	var totalBytes int64
	for _, msg := range messages {
		totalBytes += int64(len(msg.Value))
	}

	p.mu.Lock()
	p.messagesSent += int64(len(messages))
	p.bytesWritten += totalBytes
	p.mu.Unlock()

	p.logger.Debug("batch sent",
		zap.Int("count", len(messages)),
		zap.Int64("bytes", totalBytes),
		zap.Duration("duration", duration))

	return nil
}

func (p *Producer) SendRaw(ctx context.Context, key, value []byte) error {
	msg := kafka.Message{
		Key:   key,
		Value: value,
		Time:  time.Now(),
	}

	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		p.mu.Lock()
		p.errors++
		p.mu.Unlock()
		return err
	}

	p.mu.Lock()
	p.messagesSent++
	p.bytesWritten += int64(len(value))
	p.mu.Unlock()

	return nil
}

func (p *Producer) Close() error {
	return p.writer.Close()
}

func (p *Producer) Flush(ctx context.Context) error {
	return nil
}

func (p *Producer) Stats() ProducerStats {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return ProducerStats{
		MessagesSent: p.messagesSent,
		BytesWritten: p.bytesWritten,
		Errors:       p.errors,
	}
}

func (p *Producer) signalToMessage(sig signal.Signal) (kafka.Message, error) {
	data, err := sig.ToJSON()
	if err != nil {
		return kafka.Message{}, err
	}

	return kafka.Message{
		Key:   []byte(sig.KafkaKey()),
		Value: data,
		Time:  sig.Timestamp,
	}, nil
}
