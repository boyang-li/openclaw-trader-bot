package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoad_Defaults(t *testing.T) {
	cfg, err := Load("test-service")
	require.NoError(t, err)

	assert.Equal(t, "test-service", cfg.Service.Name)
	assert.Equal(t, "development", cfg.Service.Environment)
	assert.Equal(t, "0.1.0", cfg.Service.Version)

	assert.Equal(t, []string{"localhost:9093"}, cfg.Kafka.Brokers)
	assert.Equal(t, "l1-test-service", cfg.Kafka.ClientID)
	assert.Equal(t, "l1.signals.raw", cfg.Kafka.TopicRaw)
	assert.Equal(t, "l1.signals.enriched", cfg.Kafka.TopicEnriched)
	assert.Equal(t, "l1.signals.filtered", cfg.Kafka.TopicFiltered)
	assert.Equal(t, "l1.signals.dlq", cfg.Kafka.TopicDLQ)
	assert.Equal(t, 100, cfg.Kafka.BatchSize)
	assert.Equal(t, time.Second, cfg.Kafka.BatchTimeout)
	assert.Equal(t, -1, cfg.Kafka.RequiredAcks)
	assert.Equal(t, 3, cfg.Kafka.MaxRetries)
	assert.Equal(t, 100*time.Millisecond, cfg.Kafka.RetryBackoff)
	assert.Equal(t, "snappy", cfg.Kafka.CompressionType)

	assert.Equal(t, "localhost:6379", cfg.Redis.URL)
	assert.Equal(t, "", cfg.Redis.Password)
	assert.Equal(t, 0, cfg.Redis.DB)

	assert.Equal(t, 8080, cfg.HTTP.Port)
	assert.Equal(t, 10*time.Second, cfg.HTTP.ReadTimeout)
	assert.Equal(t, 10*time.Second, cfg.HTTP.WriteTimeout)
	assert.Equal(t, 30*time.Second, cfg.HTTP.ShutdownTimeout)

	assert.Equal(t, "info", cfg.Log.Level)
	assert.Equal(t, "json", cfg.Log.Format)
}

func TestLoad_EnvOverrides(t *testing.T) {
	t.Setenv("L1_SERVICE_NAME", "env-service")
	t.Setenv("L1_SERVICE_ENVIRONMENT", "production")
	t.Setenv("L1_KAFKA_BROKERS", "broker1:9092,broker2:9092")
	t.Setenv("L1_HTTP_PORT", "9090")
	t.Setenv("L1_LOG_LEVEL", "debug")

	cfg, err := Load("default-service")
	require.NoError(t, err)

	assert.Equal(t, "env-service", cfg.Service.Name)
	assert.Equal(t, 9090, cfg.HTTP.Port)
}

func TestValidate_Valid(t *testing.T) {
	cfg := &Config{
		Service: ServiceConfig{Name: "test"},
		Kafka: KafkaConfig{
			Brokers:  []string{"localhost:9092"},
			TopicRaw: "test-topic",
		},
		HTTP: HTTPConfig{Port: 8080},
	}

	err := cfg.Validate()
	assert.NoError(t, err)
}

func TestValidate_MissingServiceName(t *testing.T) {
	cfg := &Config{
		Service: ServiceConfig{Name: ""},
		Kafka: KafkaConfig{
			Brokers:  []string{"localhost:9092"},
			TopicRaw: "test-topic",
		},
		HTTP: HTTPConfig{Port: 8080},
	}

	err := cfg.Validate()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "service.name is required")
}

func TestValidate_NoBrokers(t *testing.T) {
	cfg := &Config{
		Service: ServiceConfig{Name: "test"},
		Kafka: KafkaConfig{
			Brokers:  []string{},
			TopicRaw: "test-topic",
		},
		HTTP: HTTPConfig{Port: 8080},
	}

	err := cfg.Validate()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "kafka.brokers is required")
}

func TestValidate_NoTopicRaw(t *testing.T) {
	cfg := &Config{
		Service: ServiceConfig{Name: "test"},
		Kafka: KafkaConfig{
			Brokers:  []string{"localhost:9092"},
			TopicRaw: "",
		},
		HTTP: HTTPConfig{Port: 8080},
	}

	err := cfg.Validate()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "kafka.topic_raw is required")
}

func TestValidate_InvalidPort(t *testing.T) {
	tests := []struct {
		name string
		port int
	}{
		{"zero", 0},
		{"negative", -1},
		{"too high", 65536},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &Config{
				Service: ServiceConfig{Name: "test"},
				Kafka: KafkaConfig{
					Brokers:  []string{"localhost:9092"},
					TopicRaw: "test-topic",
				},
				HTTP: HTTPConfig{Port: tt.port},
			}

			err := cfg.Validate()
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "http.port must be between 1 and 65535")
		})
	}
}

func TestValidate_ValidPorts(t *testing.T) {
	tests := []struct {
		name string
		port int
	}{
		{"minimum", 1},
		{"standard", 8080},
		{"maximum", 65535},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &Config{
				Service: ServiceConfig{Name: "test"},
				Kafka: KafkaConfig{
					Brokers:  []string{"localhost:9092"},
					TopicRaw: "test-topic",
				},
				HTTP: HTTPConfig{Port: tt.port},
			}

			err := cfg.Validate()
			assert.NoError(t, err)
		})
	}
}

func TestLoadGDELTConfig_Defaults(t *testing.T) {
	cfg, err := LoadGDELTConfig()
	require.NoError(t, err)

	assert.Equal(t, 15*time.Minute, cfg.PollInterval)
	assert.Equal(t, 250, cfg.BatchSize)
	assert.True(t, cfg.Enabled)
	assert.Equal(t, "http://data.gdeltproject.org/gdeltv2", cfg.BaseURL)
}

func TestLoadGDELTConfig_EnvOverrides(t *testing.T) {
	t.Setenv("L1_GDELT_POLL_INTERVAL", "30m")
	t.Setenv("L1_GDELT_BATCH_SIZE", "500")
	t.Setenv("L1_GDELT_ENABLED", "false")
	t.Setenv("L1_GDELT_BASE_URL", "http://custom.gdelt.org")

	cfg, err := LoadGDELTConfig()
	require.NoError(t, err)

	assert.Equal(t, 30*time.Minute, cfg.PollInterval)
	assert.Equal(t, 500, cfg.BatchSize)
	assert.False(t, cfg.Enabled)
	assert.Equal(t, "http://custom.gdelt.org", cfg.BaseURL)
}

func TestLoadFREDConfig_Defaults(t *testing.T) {
	t.Setenv("L1_FRED_API_KEY", "test-api-key")

	cfg, err := LoadFREDConfig()
	require.NoError(t, err)

	assert.Equal(t, "test-api-key", cfg.APIKey)
	assert.Equal(t, time.Hour, cfg.PollInterval)
	assert.True(t, cfg.Enabled)
	assert.Equal(t, "https://api.stlouisfed.org/fred", cfg.BaseURL)
	assert.Contains(t, cfg.Series, "DFF")
	assert.Contains(t, cfg.Series, "T10Y2Y")
}

func TestLoadFREDConfig_MissingAPIKey(t *testing.T) {
	t.Setenv("L1_FRED_API_KEY", "")

	_, err := LoadFREDConfig()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "fred.api_key is required")
}

func TestLoadBinanceConfig_Defaults(t *testing.T) {
	cfg, err := LoadBinanceConfig()
	require.NoError(t, err)

	assert.True(t, cfg.Enabled)
	assert.Equal(t, []string{"BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT"}, cfg.Pairs)
	assert.Equal(t, 1000000.0, cfg.LargeTradeThresholdUSD)
	assert.Equal(t, 2.0, cfg.PriceChangeThresholdPct)
	assert.Equal(t, 3.0, cfg.VolumeSpikeMultiplier)
	assert.Equal(t, "https://api.binance.com", cfg.BaseURL)
	assert.Equal(t, "wss://stream.binance.com:9443", cfg.WSBaseURL)
}

func TestLoadBinanceConfig_EnvOverrides(t *testing.T) {
	t.Setenv("L1_BINANCE_LARGE_TRADE_THRESHOLD_USD", "5000000")
	t.Setenv("L1_BINANCE_PRICE_CHANGE_THRESHOLD_PCT", "5.0")
	t.Setenv("L1_BINANCE_PAIRS", "BTCUSDT,ETHUSDT,DOGEUSDT")

	cfg, err := LoadBinanceConfig()
	require.NoError(t, err)

	assert.Equal(t, 5000000.0, cfg.LargeTradeThresholdUSD)
	assert.Equal(t, 5.0, cfg.PriceChangeThresholdPct)
	assert.Equal(t, []string{"BTCUSDT", "ETHUSDT", "DOGEUSDT"}, cfg.Pairs)
}

func TestLoadWhaleAlertConfig_Defaults(t *testing.T) {
	t.Setenv("L1_WHALEALERT_API_KEY", "test-whale-key")

	cfg, err := LoadWhaleAlertConfig()
	require.NoError(t, err)

	assert.Equal(t, "test-whale-key", cfg.APIKey)
	assert.True(t, cfg.Enabled)
	assert.Equal(t, time.Minute, cfg.PollInterval)
	assert.Equal(t, int64(1000000), cfg.MinValueUSD)
	assert.Equal(t, "https://api.whale-alert.io/v1", cfg.BaseURL)
}

func TestLoadWhaleAlertConfig_MissingAPIKey(t *testing.T) {
	t.Setenv("L1_WHALEALERT_API_KEY", "")

	_, err := LoadWhaleAlertConfig()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "whalealert.api_key is required")
}

func TestLoadCOTConfig_Defaults(t *testing.T) {
	cfg, err := LoadCOTConfig()
	require.NoError(t, err)

	assert.True(t, cfg.Enabled)
	assert.Equal(t, 6*time.Hour, cfg.PollInterval)
	assert.Equal(t, "https://www.cftc.gov/files/dea/history", cfg.BaseURL)
	assert.Contains(t, cfg.Contracts, "GOLD - COMMODITY EXCHANGE INC.")
	assert.Contains(t, cfg.Contracts, "BITCOIN - CHICAGO MERCANTILE EXCHANGE")
}

func TestLoadCOTConfig_EnvOverrides(t *testing.T) {
	t.Setenv("L1_COT_ENABLED", "false")
	t.Setenv("L1_COT_POLL_INTERVAL", "12h")
	t.Setenv("L1_COT_CONTRACTS", "GOLD,SILVER,OIL")

	cfg, err := LoadCOTConfig()
	require.NoError(t, err)

	assert.False(t, cfg.Enabled)
	assert.Equal(t, 12*time.Hour, cfg.PollInterval)
	assert.Equal(t, []string{"GOLD", "SILVER", "OIL"}, cfg.Contracts)
}

func TestLoadTradingEconomicsConfig_Defaults(t *testing.T) {
	t.Setenv("L1_TRADINGECONOMICS_API_KEY", "test-te-key")

	cfg, err := LoadTradingEconomicsConfig()
	require.NoError(t, err)

	assert.Equal(t, "test-te-key", cfg.APIKey)
	assert.True(t, cfg.Enabled)
	assert.Equal(t, 5*time.Minute, cfg.PollInterval)
	assert.Equal(t, "https://api.tradingeconomics.com", cfg.BaseURL)
	assert.Contains(t, cfg.Countries, "united states")
	assert.Contains(t, cfg.Indicators, "interest rate")
}

func TestLoadTradingEconomicsConfig_MissingAPIKey(t *testing.T) {
	t.Setenv("L1_TRADINGECONOMICS_API_KEY", "")

	_, err := LoadTradingEconomicsConfig()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "tradingeconomics.api_key is required")
}

func TestLoadTelegramConfig_Defaults(t *testing.T) {
	t.Setenv("L1_TELEGRAM_BOT_TOKEN", "test-bot-token")

	cfg, err := LoadTelegramConfig()
	require.NoError(t, err)

	assert.Equal(t, "test-bot-token", cfg.BotToken)
	assert.True(t, cfg.Enabled)
	assert.Equal(t, 5*time.Second, cfg.PollInterval)
	assert.Equal(t, 30, cfg.PollTimeout)
	assert.Contains(t, cfg.Keywords, "breaking")
	assert.Contains(t, cfg.Keywords, "bitcoin")
}

func TestLoadTelegramConfig_MissingBotToken(t *testing.T) {
	t.Setenv("L1_TELEGRAM_BOT_TOKEN", "")

	_, err := LoadTelegramConfig()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "telegram.bot_token is required")
}

func TestLoadTelegramConfig_ChatIDsFromEnv(t *testing.T) {
	t.Setenv("L1_TELEGRAM_BOT_TOKEN", "test-token")
	t.Setenv("L1_TELEGRAM_CHAT_IDS", "-1001234567890,-1009876543210")

	cfg, err := LoadTelegramConfig()
	require.NoError(t, err)

	assert.Equal(t, []int64{-1001234567890, -1009876543210}, cfg.ChatIDs)
}

func TestLoadTelegramConfig_KeywordsFromEnv(t *testing.T) {
	t.Setenv("L1_TELEGRAM_BOT_TOKEN", "test-token")
	t.Setenv("L1_TELEGRAM_KEYWORDS", "urgent,critical,alert")

	cfg, err := LoadTelegramConfig()
	require.NoError(t, err)

	assert.Equal(t, []string{"urgent", "critical", "alert"}, cfg.Keywords)
}
