package main

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	ossignal "os/signal"
	"strings"
	"syscall"
	"time"

	"go.uber.org/zap"

	"github.com/openclaworg/l1-ingestion/internal/config"
	"github.com/openclaworg/l1-ingestion/internal/health"
	"github.com/openclaworg/l1-ingestion/internal/kafka"
	"github.com/openclaworg/l1-ingestion/internal/orchestrator"
	"github.com/openclaworg/l1-ingestion/internal/provider"
	"github.com/openclaworg/l1-ingestion/internal/provider/binance"
	"github.com/openclaworg/l1-ingestion/internal/provider/cot"
	"github.com/openclaworg/l1-ingestion/internal/provider/gdelt"
	"github.com/openclaworg/l1-ingestion/internal/provider/telegram"
	"github.com/openclaworg/l1-ingestion/internal/signal"
)

const (
	serviceName = "orchestrator"
	version     = "0.1.0"
)

func main() {
	logger, err := initLogger()
	if err != nil {
		panic("failed to initialize logger: " + err.Error())
	}
	defer logger.Sync()

	logger.Info("starting L1 orchestrator",
		zap.String("service", serviceName),
		zap.String("version", version),
	)

	cfg, err := config.Load(serviceName)
	if err != nil {
		logger.Fatal("failed to load configuration", zap.Error(err))
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
		logger.Fatal("failed to create Kafka producer", zap.Error(err))
	}
	defer producer.Close()

	orchCfg := orchestrator.DefaultConfig()
	orchCfg.EnableSupervision = getBoolEnv("L1_ORCHESTRATOR_SUPERVISION", false)
	orch := orchestrator.New(orchCfg, logger)

	orch.SetGlobalHandler(func(ctx context.Context, sig signal.Signal) error {
		return producer.Send(ctx, sig)
	})

	enabledProviders := getEnabledProviders()
	logger.Info("enabled providers", zap.Strings("providers", enabledProviders))

	if err := registerProviders(orch, enabledProviders, logger); err != nil {
		logger.Fatal("failed to register providers", zap.Error(err))
	}

	if err := orch.Start(ctx); err != nil {
		logger.Fatal("failed to start orchestrator", zap.Error(err))
	}

	healthServer := health.NewServer(serviceName, version, cfg.HTTP.Port, logger)
	healthServer.RegisterChecker("kafka", health.CommonCheckers{}.KafkaChecker(cfg.Kafka.Brokers))

	healthServer.RegisterChecker("orchestrator", func(ctx context.Context) health.ComponentHealth {
		summary := orch.HealthSummary()
		status := health.StatusHealthy
		if summary.UnhealthyCount > 0 {
			status = health.StatusDegraded
		}
		if summary.State != orchestrator.StateRunning {
			status = health.StatusUnhealthy
		}
		return health.ComponentHealth{
			Status:    status,
			Message:   summary.State.String(),
			LastCheck: time.Now(),
		}
	})

	http.HandleFunc("/providers", func(w http.ResponseWriter, r *http.Request) {
		summary := orch.HealthSummary()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(summary)
	})

	if err := healthServer.Start(ctx); err != nil {
		logger.Fatal("failed to start health server", zap.Error(err))
	}

	healthServer.SetReady(true)

	logger.Info("orchestrator started successfully",
		zap.Int("provider_count", len(orch.ListProviders())),
		zap.Int("port", cfg.HTTP.Port),
	)

	sigChan := make(chan os.Signal, 1)
	ossignal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	sig := <-sigChan
	logger.Info("received shutdown signal", zap.String("signal", sig.String()))

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	healthServer.SetReady(false)

	if err := orch.Stop(); err != nil {
		logger.Error("error stopping orchestrator", zap.Error(err))
	}

	if err := healthServer.Stop(shutdownCtx); err != nil {
		logger.Error("error stopping health server", zap.Error(err))
	}

	cancel()

	logger.Info("orchestrator stopped")
}

func initLogger() (*zap.Logger, error) {
	env := os.Getenv("L1_SERVICE_ENVIRONMENT")
	if env == "production" {
		return zap.NewProduction()
	}
	return zap.NewDevelopment()
}

func getEnabledProviders() []string {
	enabled := os.Getenv("L1_ORCHESTRATOR_PROVIDERS")
	if enabled == "" {
		return []string{"gdelt", "binance", "cot"}
	}
	var result []string
	for _, p := range strings.Split(enabled, ",") {
		p = strings.TrimSpace(strings.ToLower(p))
		if p != "" {
			result = append(result, p)
		}
	}
	return result
}

func getBoolEnv(key string, defaultVal bool) bool {
	val := os.Getenv(key)
	if val == "" {
		return defaultVal
	}
	return strings.ToLower(val) == "true" || val == "1"
}

func registerProviders(orch *orchestrator.Orchestrator, enabled []string, logger *zap.Logger) error {
	enabledSet := make(map[string]bool)
	for _, p := range enabled {
		enabledSet[p] = true
	}

	if enabledSet["gdelt"] {
		gdeltCfg, err := config.LoadGDELTConfig()
		if err != nil {
			logger.Warn("failed to load GDELT config, skipping", zap.Error(err))
		} else if gdeltCfg.Enabled {
			p := gdelt.New(gdelt.Config{
				BaseURL:      gdeltCfg.BaseURL,
				PollInterval: gdeltCfg.PollInterval,
				BatchSize:    gdeltCfg.BatchSize,
				Enabled:      gdeltCfg.Enabled,
			}, logger)
			if err := orch.Register(p); err != nil {
				return err
			}
		}
	}

	if enabledSet["binance"] {
		binanceCfg, err := config.LoadBinanceConfig()
		if err != nil {
			logger.Warn("failed to load Binance config, skipping", zap.Error(err))
		} else if binanceCfg.Enabled {
			p := binance.New(binance.Config{
				WSBaseURL:               binanceCfg.WSBaseURL,
				RESTBaseURL:             binanceCfg.BaseURL,
				Pairs:                   binanceCfg.Pairs,
				LargeTradeThresholdUSD:  binanceCfg.LargeTradeThresholdUSD,
				PriceChangeThresholdPct: binanceCfg.PriceChangeThresholdPct,
				VolumeSpikeMultiplier:   binanceCfg.VolumeSpikeMultiplier,
				Enabled:                 binanceCfg.Enabled,
			}, logger)
			if err := orch.Register(p); err != nil {
				return err
			}
		}
	}

	if enabledSet["cot"] {
		cotCfg, err := config.LoadCOTConfig()
		if err != nil {
			logger.Warn("failed to load COT config, skipping", zap.Error(err))
		} else if cotCfg.Enabled {
			p := cot.New(cot.Config{
				BaseURL:      cotCfg.BaseURL,
				PollInterval: cotCfg.PollInterval,
				Contracts:    cotCfg.Contracts,
				Enabled:      cotCfg.Enabled,
			}, logger)
			if err := orch.Register(p); err != nil {
				return err
			}
		}
	}

	if enabledSet["telegram"] {
		telegramCfg, err := config.LoadTelegramConfig()
		if err != nil {
			logger.Warn("failed to load Telegram config, skipping", zap.Error(err))
		} else if telegramCfg.Enabled {
			p, err := telegram.New(telegram.Config{
				BotToken:     telegramCfg.BotToken,
				ChatIDs:      telegramCfg.ChatIDs,
				Keywords:     telegramCfg.Keywords,
				PollInterval: telegramCfg.PollInterval,
				PollTimeout:  telegramCfg.PollTimeout,
				Enabled:      telegramCfg.Enabled,
			}, logger)
			if err != nil {
				logger.Warn("failed to create Telegram provider, skipping", zap.Error(err))
			} else if err := orch.Register(p); err != nil {
				return err
			}
		}
	}

	return nil
}

var _ provider.SignalHandler = func(ctx context.Context, sig signal.Signal) error { return nil }
