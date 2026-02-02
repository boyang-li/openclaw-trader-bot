package provider

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/openclaworg/l1-ingestion/internal/signal"
	"go.uber.org/zap"
)

const maxConsecutiveErrors = 5

type BaseProvider struct {
	config            ProviderConfig
	handlers          []SignalHandler
	startTime         time.Time
	stopCh            chan struct{}
	stopped           atomic.Bool
	mu                sync.RWMutex
	logger            *zap.Logger
	lastSuccess       time.Time
	lastError         string
	errorCount        int64
	consecutiveErrors int64
	successCount      int64
	signalCount       int64
}

func NewBaseProvider(config ProviderConfig, logger *zap.Logger) *BaseProvider {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &BaseProvider{
		config:   config,
		handlers: make([]SignalHandler, 0),
		stopCh:   make(chan struct{}),
		logger:   logger,
	}
}

func (b *BaseProvider) Name() string {
	return b.config.Name
}

func (b *BaseProvider) Category() signal.Category {
	return b.config.Category
}

func (b *BaseProvider) Subscribe(handler SignalHandler) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.handlers = append(b.handlers, handler)
	return nil
}

func (b *BaseProvider) Health() HealthStatus {
	b.mu.RLock()
	defer b.mu.RUnlock()

	var uptime float64
	if !b.startTime.IsZero() {
		uptime = time.Since(b.startTime).Seconds()
	}

	return HealthStatus{
		Healthy:       b.consecutiveErrors < maxConsecutiveErrors,
		LastSuccess:   b.lastSuccess,
		LastError:     b.lastError,
		ErrorCount:    b.errorCount,
		SuccessCount:  b.successCount,
		SignalCount:   b.signalCount,
		UptimeSeconds: uptime,
	}
}

func (b *BaseProvider) Stop() error {
	if b.stopped.CompareAndSwap(false, true) {
		close(b.stopCh)
	}
	return nil
}

func (b *BaseProvider) Emit(ctx context.Context, sig signal.Signal) error {
	b.mu.RLock()
	handlers := make([]SignalHandler, len(b.handlers))
	copy(handlers, b.handlers)
	b.mu.RUnlock()

	if len(handlers) == 0 {
		return ErrNoHandlers
	}

	for _, handler := range handlers {
		if err := handler(ctx, sig); err != nil {
			b.RecordError(err)
			return &ProviderError{Provider: b.config.Name, Op: "emit", Err: err}
		}
	}

	b.RecordSuccess()
	b.mu.Lock()
	b.signalCount++
	b.mu.Unlock()

	return nil
}

func (b *BaseProvider) RecordSuccess() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.lastSuccess = time.Now()
	b.successCount++
	b.consecutiveErrors = 0
}

func (b *BaseProvider) RecordError(err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.lastError = err.Error()
	b.errorCount++
	b.consecutiveErrors++
}

func (b *BaseProvider) MarkStarted() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.startTime = time.Now()
}

func (b *BaseProvider) IsStopped() bool {
	return b.stopped.Load()
}

func (b *BaseProvider) StopChannel() <-chan struct{} {
	return b.stopCh
}

func (b *BaseProvider) Logger() *zap.Logger {
	return b.logger
}

func (b *BaseProvider) Config() ProviderConfig {
	return b.config
}
