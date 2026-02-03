package kafka

import (
	"context"

	"github.com/openclaworg/l1-ingestion/internal/provider"
	"github.com/openclaworg/l1-ingestion/internal/signal"
)

func NewSignalHandler(producer *Producer) provider.SignalHandler {
	return func(ctx context.Context, sig signal.Signal) error {
		return producer.Send(ctx, sig)
	}
}
