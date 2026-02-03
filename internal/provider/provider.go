package provider

import (
	"context"
	"time"

	"github.com/openclaworg/l1-ingestion/internal/signal"
)

type SignalHandler func(ctx context.Context, sig signal.Signal) error

type Provider interface {
	Name() string
	Category() signal.Category
	Start(ctx context.Context) error
	Stop() error
	Subscribe(handler SignalHandler) error
	Health() HealthStatus
}

type HealthStatus struct {
	Healthy       bool           `json:"healthy"`
	LastSuccess   time.Time      `json:"last_success,omitempty"`
	LastError     string         `json:"last_error,omitempty"`
	ErrorCount    int64          `json:"error_count"`
	SuccessCount  int64          `json:"success_count"`
	SignalCount   int64          `json:"signal_count"`
	UptimeSeconds float64        `json:"uptime_seconds"`
	Extra         map[string]any `json:"extra,omitempty"`
}

type ProviderConfig struct {
	Name           string
	Category       signal.Category
	PollInterval   time.Duration
	RequestTimeout time.Duration
	MaxRetries     int
	RetryBackoff   time.Duration
}

func DefaultProviderConfig(name string, category signal.Category) ProviderConfig {
	return ProviderConfig{
		Name:           name,
		Category:       category,
		PollInterval:   15 * time.Minute,
		RequestTimeout: 30 * time.Second,
		MaxRetries:     3,
		RetryBackoff:   time.Second,
	}
}
