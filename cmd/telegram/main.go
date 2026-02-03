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
	"github.com/openclaworg/l1-ingestion/internal/provider/telegram"
	"github.com/openclaworg/l1-ingestion/internal/signal"
)

const (
	serviceName = "telegram"
	version     = "0.1.0"
)

func main() {
	logger, err := initLogger()
	if err != nil {
		panic("failed to initialize logger: " + err.Error())
	}
	defer logger.Sync()

	logger.Info("Starting Telegram ingestor",
		zap.String("service", serviceName),
		zap.String("version", version),
	)

	cfg, err := config.Load(serviceName)
	if err != nil {
		logger.Fatal("Failed to load configuration", zap.Error(err))
	}

	// Use standard port 8080 inside container (mapped externally via docker-compose)
	cfg.HTTP.Port = 8080

	telegramCfg, err := config.LoadTelegramConfig()
	if err != nil {
		logger.Fatal("Failed to load Telegram configuration", zap.Error(err))
	}

	if !telegramCfg.Enabled {
		logger.Info("Telegram ingestor is disabled, exiting")
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

	providerCfg := telegram.Config{
		BotToken:     telegramCfg.BotToken,
		PollInterval: telegramCfg.PollInterval,
		PollTimeout:  telegramCfg.PollTimeout,
		ChatIDs:      telegramCfg.ChatIDs,
		Keywords:     telegramCfg.Keywords,
		Enabled:      telegramCfg.Enabled,
	}

	telegramProvider, err := telegram.New(providerCfg, logger)
	if err != nil {
		logger.Fatal("Failed to create Telegram provider", zap.Error(err))
	}

	signalHandler := func(ctx context.Context, sig signal.Signal) error {
		return producer.Send(ctx, sig)
	}

	if err := telegramProvider.Subscribe(signalHandler); err != nil {
		logger.Fatal("Failed to subscribe to provider", zap.Error(err))
	}

	if err := telegramProvider.Start(ctx); err != nil {
		logger.Fatal("Failed to start Telegram provider", zap.Error(err))
	}
	defer telegramProvider.Stop()

	healthServer := health.NewServer(serviceName, version, cfg.HTTP.Port, logger)

	healthServer.RegisterChecker("kafka", func(ctx context.Context) health.ComponentHealth {
		return health.ComponentHealth{
			Status:    health.StatusHealthy,
			LastCheck: time.Now(),
		}
	})

	healthServer.RegisterChecker("telegram", func(ctx context.Context) health.ComponentHealth {
		h := telegramProvider.Health()
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

	logger.Info("Telegram ingestor started successfully",
		zap.Duration("poll_interval", telegramCfg.PollInterval),
		zap.Int("chat_filters", len(telegramCfg.ChatIDs)),
		zap.Int("keywords", len(telegramCfg.Keywords)),
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

	logger.Info("Telegram ingestor stopped")
}

func initLogger() (*zap.Logger, error) {
	env := os.Getenv("L1_SERVICE_ENVIRONMENT")
	if env == "production" {
		return zap.NewProduction()
	}
	return zap.NewDevelopment()
}

var _ provider.SignalHandler = func(ctx context.Context, sig signal.Signal) error { return nil }
