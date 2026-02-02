package main

import (
	"context"
	"os"
	ossignal "os/signal"
	"syscall"
	"time"

	"go.uber.org/zap"

	"github.com/openclaworg/l1-ingestion/internal/config"
	"github.com/openclaworg/l1-ingestion/internal/health"
	"github.com/openclaworg/l1-ingestion/internal/kafka"
	"github.com/openclaworg/l1-ingestion/internal/provider"
	"github.com/openclaworg/l1-ingestion/internal/provider/cot"
	"github.com/openclaworg/l1-ingestion/internal/signal"
)

const (
	serviceName = "cot"
	version     = "0.1.0"
)

func main() {
	logger, err := initLogger()
	if err != nil {
		panic("failed to initialize logger: " + err.Error())
	}
	defer logger.Sync()

	logger.Info("Starting CME COT ingestor",
		zap.String("service", serviceName),
		zap.String("version", version),
	)

	cfg, err := config.Load(serviceName)
	if err != nil {
		logger.Fatal("Failed to load configuration", zap.Error(err))
	}

	cotCfg, err := config.LoadCOTConfig()
	if err != nil {
		logger.Fatal("Failed to load COT configuration", zap.Error(err))
	}

	if !cotCfg.Enabled {
		logger.Info("CME COT ingestor is disabled, exiting")
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	kafkaCfg := kafka.DefaultConfig()
	kafkaCfg.Brokers = cfg.Kafka.Brokers
	kafkaCfg.Topic = cfg.Kafka.TopicRaw
	kafkaCfg.BatchSize = cfg.Kafka.BatchSize
	kafkaCfg.BatchTimeout = cfg.Kafka.BatchTimeout

	producer, err := kafka.NewProducer(kafkaCfg, logger)
	if err != nil {
		logger.Fatal("Failed to create Kafka producer", zap.Error(err))
	}
	defer producer.Close()

	providerCfg := cot.Config{
		BaseURL:      cotCfg.BaseURL,
		PollInterval: cotCfg.PollInterval,
		Contracts:    cotCfg.Contracts,
		Enabled:      cotCfg.Enabled,
	}

	cotProvider := cot.New(providerCfg, logger)

	signalHandler := func(ctx context.Context, sig signal.Signal) error {
		return producer.Send(ctx, sig)
	}

	if err := cotProvider.Subscribe(signalHandler); err != nil {
		logger.Fatal("Failed to subscribe to provider", zap.Error(err))
	}

	if err := cotProvider.Start(ctx); err != nil {
		logger.Fatal("Failed to start COT provider", zap.Error(err))
	}
	defer cotProvider.Stop()

	healthServer := health.NewServer(serviceName, version, cfg.HTTP.Port, logger)

	healthServer.RegisterChecker("kafka", func(ctx context.Context) health.ComponentHealth {
		return health.ComponentHealth{
			Status:    health.StatusHealthy,
			LastCheck: time.Now(),
		}
	})

	healthServer.RegisterChecker("cot", func(ctx context.Context) health.ComponentHealth {
		h := cotProvider.Health()
		status := health.StatusHealthy
		if !h.Healthy {
			status = health.StatusUnhealthy
		}
		return health.ComponentHealth{
			Status:    status,
			Message:   h.LastError,
			LastCheck: time.Now(),
		}
	})

	if err := healthServer.Start(ctx); err != nil {
		logger.Fatal("Failed to start health server", zap.Error(err))
	}

	healthServer.SetReady(true)

	logger.Info("CME COT ingestor started successfully",
		zap.Duration("poll_interval", cotCfg.PollInterval),
		zap.Int("contracts_tracked", len(cotCfg.Contracts)),
		zap.Int("port", cfg.HTTP.Port),
	)

	sigChan := make(chan os.Signal, 1)
	ossignal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	sig := <-sigChan
	logger.Info("Received shutdown signal", zap.String("signal", sig.String()))

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	healthServer.SetReady(false)

	if err := healthServer.Stop(shutdownCtx); err != nil {
		logger.Error("Error stopping health server", zap.Error(err))
	}

	cancel()

	logger.Info("CME COT ingestor stopped")
}

func initLogger() (*zap.Logger, error) {
	env := os.Getenv("L1_SERVICE_ENVIRONMENT")
	if env == "production" {
		return zap.NewProduction()
	}
	return zap.NewDevelopment()
}

var _ provider.SignalHandler = func(ctx context.Context, sig signal.Signal) error { return nil }
