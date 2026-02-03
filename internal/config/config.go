// Package config provides configuration management for L1 ingestors.
// It uses Viper for flexible configuration from files, environment variables, and defaults.
package config

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/viper"
)

// Config holds all configuration for an L1 ingestor service.
type Config struct {
	// Service identification
	Service ServiceConfig `mapstructure:"service"`

	// Kafka configuration
	Kafka KafkaConfig `mapstructure:"kafka"`

	// Redis configuration
	Redis RedisConfig `mapstructure:"redis"`

	// HTTP server configuration
	HTTP HTTPConfig `mapstructure:"http"`

	// Logging configuration
	Log LogConfig `mapstructure:"log"`

	// Provider-specific configuration (loaded separately)
	Provider map[string]interface{} `mapstructure:"provider"`
}

// ServiceConfig holds service identification settings.
type ServiceConfig struct {
	Name        string `mapstructure:"name"`
	Environment string `mapstructure:"environment"`
	Version     string `mapstructure:"version"`
}

// KafkaConfig holds Kafka connection and producer settings.
type KafkaConfig struct {
	Brokers         []string      `mapstructure:"brokers"`
	ClientID        string        `mapstructure:"client_id"`
	TopicRaw        string        `mapstructure:"topic_raw"`
	TopicEnriched   string        `mapstructure:"topic_enriched"`
	TopicFiltered   string        `mapstructure:"topic_filtered"`
	TopicDLQ        string        `mapstructure:"topic_dlq"`
	BatchSize       int           `mapstructure:"batch_size"`
	BatchTimeout    time.Duration `mapstructure:"batch_timeout"`
	RequiredAcks    int           `mapstructure:"required_acks"`
	MaxRetries      int           `mapstructure:"max_retries"`
	RetryBackoff    time.Duration `mapstructure:"retry_backoff"`
	CompressionType string        `mapstructure:"compression_type"`
}

// RedisConfig holds Redis connection settings.
type RedisConfig struct {
	URL      string `mapstructure:"url"`
	Password string `mapstructure:"password"`
	DB       int    `mapstructure:"db"`
}

// HTTPConfig holds HTTP server settings.
type HTTPConfig struct {
	Port            int           `mapstructure:"port"`
	ReadTimeout     time.Duration `mapstructure:"read_timeout"`
	WriteTimeout    time.Duration `mapstructure:"write_timeout"`
	ShutdownTimeout time.Duration `mapstructure:"shutdown_timeout"`
}

// LogConfig holds logging settings.
type LogConfig struct {
	Level  string `mapstructure:"level"`
	Format string `mapstructure:"format"`
}

// GDELTConfig holds GDELT-specific configuration.
type GDELTConfig struct {
	PollInterval time.Duration `mapstructure:"poll_interval"`
	BatchSize    int           `mapstructure:"batch_size"`
	Enabled      bool          `mapstructure:"enabled"`
	BaseURL      string        `mapstructure:"base_url"`
}

// FREDConfig holds FRED-specific configuration.
type FREDConfig struct {
	APIKey       string        `mapstructure:"api_key"`
	PollInterval time.Duration `mapstructure:"poll_interval"`
	Enabled      bool          `mapstructure:"enabled"`
	Series       []string      `mapstructure:"series"`
	BaseURL      string        `mapstructure:"base_url"`
}

// BinanceConfig holds Binance-specific configuration.
type BinanceConfig struct {
	APIKey                  string   `mapstructure:"api_key"`
	APISecret               string   `mapstructure:"api_secret"`
	Enabled                 bool     `mapstructure:"enabled"`
	Pairs                   []string `mapstructure:"pairs"`
	LargeTradeThresholdUSD  float64  `mapstructure:"large_trade_threshold_usd"`
	PriceChangeThresholdPct float64  `mapstructure:"price_change_threshold_pct"`
	VolumeSpikeMultiplier   float64  `mapstructure:"volume_spike_multiplier"`
	BaseURL                 string   `mapstructure:"base_url"`
	WSBaseURL               string   `mapstructure:"ws_base_url"`
}

// WhaleAlertConfig holds Whale Alert-specific configuration.
type WhaleAlertConfig struct {
	APIKey           string        `mapstructure:"api_key"`
	PollInterval     time.Duration `mapstructure:"poll_interval"`
	MinValueUSD      int64         `mapstructure:"min_value_usd"`
	Enabled          bool          `mapstructure:"enabled"`
	Blockchains      []string      `mapstructure:"blockchains"`
	TransactionTypes []string      `mapstructure:"transaction_types"`
	BaseURL          string        `mapstructure:"base_url"`
}

// COTConfig holds CME COT-specific configuration.
type COTConfig struct {
	PollInterval time.Duration `mapstructure:"poll_interval"`
	Contracts    []string      `mapstructure:"contracts"`
	Enabled      bool          `mapstructure:"enabled"`
	BaseURL      string        `mapstructure:"base_url"`
}

// TradingEconomicsConfig holds Trading Economics-specific configuration.
type TradingEconomicsConfig struct {
	APIKey       string        `mapstructure:"api_key"`
	PollInterval time.Duration `mapstructure:"poll_interval"`
	Countries    []string      `mapstructure:"countries"`
	Indicators   []string      `mapstructure:"indicators"`
	Enabled      bool          `mapstructure:"enabled"`
	BaseURL      string        `mapstructure:"base_url"`
}

// TelegramConfig holds Telegram-specific configuration.
type TelegramConfig struct {
	BotToken     string        `mapstructure:"bot_token"`
	PollInterval time.Duration `mapstructure:"poll_interval"`
	PollTimeout  int           `mapstructure:"poll_timeout"`
	ChatIDs      []int64       `mapstructure:"chat_ids"`
	Keywords     []string      `mapstructure:"keywords"`
	Enabled      bool          `mapstructure:"enabled"`
}

// Load reads configuration from file, environment, and defaults.
func Load(serviceName string) (*Config, error) {
	v := viper.New()

	// Set defaults
	setDefaults(v, serviceName)

	// Configuration file settings
	v.SetConfigName("config")
	v.SetConfigType("yaml")
	v.AddConfigPath(".")
	v.AddConfigPath("./config")
	v.AddConfigPath("/etc/l1-ingestion")

	// Environment variable settings
	v.SetEnvPrefix("L1")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	// Read config file (optional)
	if err := v.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return nil, fmt.Errorf("error reading config file: %w", err)
		}
		// Config file not found is OK, we'll use env vars and defaults
	}

	// Unmarshal into struct
	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("error unmarshaling config: %w", err)
	}

	// Validate configuration
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	return &cfg, nil
}

// setDefaults sets default values for all configuration options.
func setDefaults(v *viper.Viper, serviceName string) {
	// Service defaults
	v.SetDefault("service.name", serviceName)
	v.SetDefault("service.environment", "development")
	v.SetDefault("service.version", "0.1.0")

	// Kafka defaults
	v.SetDefault("kafka.brokers", []string{"localhost:9093"})
	v.SetDefault("kafka.client_id", fmt.Sprintf("l1-%s", serviceName))
	v.SetDefault("kafka.topic_raw", "l1.signals.raw")
	v.SetDefault("kafka.topic_enriched", "l1.signals.enriched")
	v.SetDefault("kafka.topic_filtered", "l1.signals.filtered")
	v.SetDefault("kafka.topic_dlq", "l1.signals.dlq")
	v.SetDefault("kafka.batch_size", 100)
	v.SetDefault("kafka.batch_timeout", "1s")
	v.SetDefault("kafka.required_acks", -1) // all replicas
	v.SetDefault("kafka.max_retries", 3)
	v.SetDefault("kafka.retry_backoff", "100ms")
	v.SetDefault("kafka.compression_type", "snappy")

	// Redis defaults
	v.SetDefault("redis.url", "localhost:6379")
	v.SetDefault("redis.password", "")
	v.SetDefault("redis.db", 0)

	// HTTP defaults
	v.SetDefault("http.port", 8080)
	v.SetDefault("http.read_timeout", "10s")
	v.SetDefault("http.write_timeout", "10s")
	v.SetDefault("http.shutdown_timeout", "30s")

	// Log defaults
	v.SetDefault("log.level", "info")
	v.SetDefault("log.format", "json")
}

// Validate checks that the configuration is valid.
func (c *Config) Validate() error {
	if c.Service.Name == "" {
		return fmt.Errorf("service.name is required")
	}

	if len(c.Kafka.Brokers) == 0 {
		return fmt.Errorf("kafka.brokers is required")
	}

	if c.Kafka.TopicRaw == "" {
		return fmt.Errorf("kafka.topic_raw is required")
	}

	if c.HTTP.Port <= 0 || c.HTTP.Port > 65535 {
		return fmt.Errorf("http.port must be between 1 and 65535")
	}

	return nil
}

// LoadGDELTConfig loads GDELT-specific configuration.
func LoadGDELTConfig() (*GDELTConfig, error) {
	v := viper.New()

	// Set defaults
	v.SetDefault("gdelt.poll_interval", "15m")
	v.SetDefault("gdelt.batch_size", 250)
	v.SetDefault("gdelt.enabled", true)
	v.SetDefault("gdelt.base_url", "http://data.gdeltproject.org/gdeltv2")

	// Environment variables
	v.SetEnvPrefix("L1")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	var cfg GDELTConfig
	if err := v.UnmarshalKey("gdelt", &cfg); err != nil {
		return nil, fmt.Errorf("error unmarshaling GDELT config: %w", err)
	}

	return &cfg, nil
}

// LoadFREDConfig loads FRED-specific configuration.
func LoadFREDConfig() (*FREDConfig, error) {
	v := viper.New()

	// Set defaults
	v.SetDefault("fred.poll_interval", "1h")
	v.SetDefault("fred.enabled", true)
	v.SetDefault("fred.base_url", "https://api.stlouisfed.org/fred")
	v.SetDefault("fred.series", []string{
		"DFF",          // Federal Funds Rate
		"T10Y2Y",       // 10Y-2Y Spread
		"UNRATE",       // Unemployment Rate
		"CPIAUCSL",     // CPI
		"GDP",          // GDP
		"FEDFUNDS",     // Federal Funds
		"MORTGAGE30US", // 30Y Mortgage
		"DTWEXBGS",     // Dollar Index
	})

	// Environment variables
	v.SetEnvPrefix("L1")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	// Explicit bindings for nested keys (Viper quirk: UnmarshalKey doesn't see AutomaticEnv)
	_ = v.BindEnv("fred.api_key", "L1_FRED_API_KEY")
	_ = v.BindEnv("fred.poll_interval", "L1_FRED_POLL_INTERVAL")
	_ = v.BindEnv("fred.enabled", "L1_FRED_ENABLED")
	_ = v.BindEnv("fred.base_url", "L1_FRED_BASE_URL")

	var cfg FREDConfig
	if err := v.UnmarshalKey("fred", &cfg); err != nil {
		return nil, fmt.Errorf("error unmarshaling FRED config: %w", err)
	}

	// Manual override: Viper's UnmarshalKey doesn't properly read bound env vars
	if v.IsSet("fred.api_key") {
		cfg.APIKey = v.GetString("fred.api_key")
	}
	if v.IsSet("fred.poll_interval") {
		cfg.PollInterval = v.GetDuration("fred.poll_interval")
	}
	if v.IsSet("fred.enabled") {
		cfg.Enabled = v.GetBool("fred.enabled")
	}
	if v.IsSet("fred.base_url") {
		cfg.BaseURL = v.GetString("fred.base_url")
	}

	if cfg.APIKey == "" {
		return nil, fmt.Errorf("fred.api_key is required")
	}

	return &cfg, nil
}

// LoadBinanceConfig loads Binance-specific configuration.
func LoadBinanceConfig() (*BinanceConfig, error) {
	v := viper.New()

	// Set defaults
	v.SetDefault("binance.enabled", true)
	v.SetDefault("binance.pairs", []string{"BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT"})
	v.SetDefault("binance.large_trade_threshold_usd", 1000000.0)
	v.SetDefault("binance.price_change_threshold_pct", 2.0)
	v.SetDefault("binance.volume_spike_multiplier", 3.0)
	v.SetDefault("binance.base_url", "https://api.binance.com")
	v.SetDefault("binance.ws_base_url", "wss://stream.binance.com:9443")

	// Environment variables
	v.SetEnvPrefix("L1")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	// Explicit bindings for nested keys (Viper quirk: UnmarshalKey doesn't see AutomaticEnv)
	_ = v.BindEnv("binance.large_trade_threshold_usd", "L1_BINANCE_LARGE_TRADE_THRESHOLD_USD")
	_ = v.BindEnv("binance.price_change_threshold_pct", "L1_BINANCE_PRICE_CHANGE_THRESHOLD_PCT")
	_ = v.BindEnv("binance.pairs", "L1_BINANCE_PAIRS")

	var cfg BinanceConfig
	if err := v.UnmarshalKey("binance", &cfg); err != nil {
		return nil, fmt.Errorf("error unmarshaling Binance config: %w", err)
	}

	// Manual override: Viper's UnmarshalKey doesn't properly read bound env vars
	// We must use Get* methods directly to read the environment variable values
	if v.IsSet("binance.large_trade_threshold_usd") {
		cfg.LargeTradeThresholdUSD = v.GetFloat64("binance.large_trade_threshold_usd")
	}
	if v.IsSet("binance.price_change_threshold_pct") {
		cfg.PriceChangeThresholdPct = v.GetFloat64("binance.price_change_threshold_pct")
	}
	if v.IsSet("binance.pairs") {
		if pairs := v.GetStringSlice("binance.pairs"); len(pairs) > 0 {
			// Handle comma-separated string from env var (e.g., "BTCUSDT,ETHUSDT")
			if len(pairs) == 1 && strings.Contains(pairs[0], ",") {
				cfg.Pairs = strings.Split(pairs[0], ",")
			} else {
				cfg.Pairs = pairs
			}
		}
	}

	return &cfg, nil
}

// LoadWhaleAlertConfig loads Whale Alert-specific configuration.
func LoadWhaleAlertConfig() (*WhaleAlertConfig, error) {
	v := viper.New()

	// Set defaults
	v.SetDefault("whalealert.enabled", true)
	v.SetDefault("whalealert.poll_interval", "1m")
	v.SetDefault("whalealert.min_value_usd", 1000000)
	v.SetDefault("whalealert.base_url", "https://api.whale-alert.io/v1")
	v.SetDefault("whalealert.blockchains", []string{})
	v.SetDefault("whalealert.transaction_types", []string{})

	// Environment variables
	v.SetEnvPrefix("L1")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	var cfg WhaleAlertConfig
	if err := v.UnmarshalKey("whalealert", &cfg); err != nil {
		return nil, fmt.Errorf("error unmarshaling Whale Alert config: %w", err)
	}

	if cfg.APIKey == "" {
		return nil, fmt.Errorf("whalealert.api_key is required")
	}

	return &cfg, nil
}

// LoadCOTConfig loads CME COT-specific configuration.
func LoadCOTConfig() (*COTConfig, error) {
	v := viper.New()

	v.SetDefault("cot.enabled", true)
	v.SetDefault("cot.poll_interval", "6h")
	v.SetDefault("cot.base_url", "https://www.cftc.gov/files/dea/history")
	v.SetDefault("cot.contracts", []string{
		"GOLD - COMMODITY EXCHANGE INC.",
		"SILVER - COMMODITY EXCHANGE INC.",
		"WTI CRUDE OIL - NEW YORK MERCANTILE EXCHANGE",
		"E-MINI S&P 500 - CHICAGO MERCANTILE EXCHANGE",
		"EURO FX - CHICAGO MERCANTILE EXCHANGE",
		"BITCOIN - CHICAGO MERCANTILE EXCHANGE",
	})

	v.SetEnvPrefix("L1")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	var cfg COTConfig
	if err := v.UnmarshalKey("cot", &cfg); err != nil {
		return nil, fmt.Errorf("error unmarshaling COT config: %w", err)
	}

	return &cfg, nil
}

// LoadTradingEconomicsConfig loads Trading Economics-specific configuration.
func LoadTradingEconomicsConfig() (*TradingEconomicsConfig, error) {
	v := viper.New()

	v.SetDefault("tradingeconomics.enabled", true)
	v.SetDefault("tradingeconomics.poll_interval", "5m")
	v.SetDefault("tradingeconomics.base_url", "https://api.tradingeconomics.com")
	v.SetDefault("tradingeconomics.countries", []string{
		"united states", "china", "euro area", "japan", "germany",
		"united kingdom", "france", "india", "brazil", "russia",
	})
	v.SetDefault("tradingeconomics.indicators", []string{
		"interest rate", "inflation rate", "gdp growth rate",
		"unemployment rate", "balance of trade",
	})

	v.SetEnvPrefix("L1")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	var cfg TradingEconomicsConfig
	if err := v.UnmarshalKey("tradingeconomics", &cfg); err != nil {
		return nil, fmt.Errorf("error unmarshaling Trading Economics config: %w", err)
	}

	if cfg.APIKey == "" {
		return nil, fmt.Errorf("tradingeconomics.api_key is required")
	}

	return &cfg, nil
}

// LoadTelegramConfig loads Telegram-specific configuration.
func LoadTelegramConfig() (*TelegramConfig, error) {
	v := viper.New()

	v.SetDefault("telegram.enabled", true)
	v.SetDefault("telegram.poll_interval", "5s")
	v.SetDefault("telegram.poll_timeout", 30)
	v.SetDefault("telegram.chat_ids", []int64{})
	v.SetDefault("telegram.keywords", []string{
		"breaking", "urgent", "alert", "warning",
		"military", "attack", "strike", "explosion",
		"sanction", "tariff", "embargo",
		"fed", "ecb", "boj", "rate", "inflation",
		"bitcoin", "ethereum", "crypto", "whale",
	})

	v.SetEnvPrefix("L1")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	_ = v.BindEnv("telegram.bot_token", "L1_TELEGRAM_BOT_TOKEN")
	_ = v.BindEnv("telegram.enabled", "L1_TELEGRAM_ENABLED")
	_ = v.BindEnv("telegram.poll_interval", "L1_TELEGRAM_POLL_INTERVAL")
	_ = v.BindEnv("telegram.poll_timeout", "L1_TELEGRAM_POLL_TIMEOUT")
	_ = v.BindEnv("telegram.chat_ids", "L1_TELEGRAM_CHAT_IDS")
	_ = v.BindEnv("telegram.keywords", "L1_TELEGRAM_KEYWORDS")

	var cfg TelegramConfig
	if err := v.UnmarshalKey("telegram", &cfg); err != nil {
		return nil, fmt.Errorf("error unmarshaling Telegram config: %w", err)
	}

	if v.IsSet("telegram.bot_token") {
		cfg.BotToken = v.GetString("telegram.bot_token")
	}
	if v.IsSet("telegram.enabled") {
		cfg.Enabled = v.GetBool("telegram.enabled")
	}
	if v.IsSet("telegram.poll_interval") {
		cfg.PollInterval = v.GetDuration("telegram.poll_interval")
	}
	if v.IsSet("telegram.poll_timeout") {
		cfg.PollTimeout = v.GetInt("telegram.poll_timeout")
	}
	if v.IsSet("telegram.chat_ids") {
		chatIDStrs := v.GetStringSlice("telegram.chat_ids")
		if len(chatIDStrs) == 1 && strings.Contains(chatIDStrs[0], ",") {
			chatIDStrs = strings.Split(chatIDStrs[0], ",")
		}
		cfg.ChatIDs = make([]int64, 0, len(chatIDStrs))
		for _, s := range chatIDStrs {
			s = strings.TrimSpace(s)
			if id, err := strconv.ParseInt(s, 10, 64); err == nil {
				cfg.ChatIDs = append(cfg.ChatIDs, id)
			}
		}
	}

	if cfg.BotToken == "" {
		return nil, fmt.Errorf("telegram.bot_token is required")
	}

	return &cfg, nil
}
