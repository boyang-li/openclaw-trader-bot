// Package fred provides a provider for Federal Reserve Economic Data (FRED).
// It polls the FRED API for macro-economic indicators and releases.
package fred

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/openclaworg/l1-ingestion/internal/provider"
	"github.com/openclaworg/l1-ingestion/internal/signal"
)

const (
	DefaultBaseURL      = "https://api.stlouisfed.org/fred"
	DefaultPollInterval = 1 * time.Hour
)

// SeriesMetadata holds metadata about a FRED series for signal generation.
type SeriesMetadata struct {
	ID        string
	Title     string
	Tags      []string
	Sentiment func(value, prevValue float64) float64
	Urgency   signal.UrgencyLevel
}

// DefaultSeries returns the default set of economic indicators to track.
func DefaultSeries() []SeriesMetadata {
	return []SeriesMetadata{
		{
			ID:    "DFF",
			Title: "Federal Funds Effective Rate",
			Tags:  []string{"interest-rate", "fed", "monetary-policy"},
			Sentiment: func(val, prev float64) float64 {
				if val > prev {
					return -0.3 // Rising rates = slightly negative
				}
				return 0.2
			},
			Urgency: signal.UrgencyMedium,
		},
		{
			ID:    "T10Y2Y",
			Title: "10-Year Treasury Constant Maturity Minus 2-Year",
			Tags:  []string{"yield-curve", "recession-indicator", "treasury"},
			Sentiment: func(val, _ float64) float64 {
				if val < 0 {
					return -0.7 // Inverted yield curve = negative
				}
				return 0.3
			},
			Urgency: signal.UrgencyHigh,
		},
		{
			ID:    "UNRATE",
			Title: "Unemployment Rate",
			Tags:  []string{"employment", "labor-market"},
			Sentiment: func(val, prev float64) float64 {
				if val > prev {
					return -0.5 // Rising unemployment = negative
				}
				return 0.3
			},
			Urgency: signal.UrgencyMedium,
		},
		{
			ID:    "CPIAUCSL",
			Title: "Consumer Price Index for All Urban Consumers",
			Tags:  []string{"inflation", "consumer-prices", "cpi"},
			Sentiment: func(val, prev float64) float64 {
				change := (val - prev) / prev * 100
				if change > 0.3 {
					return -0.4 // High inflation = negative
				}
				return 0.1
			},
			Urgency: signal.UrgencyMedium,
		},
		{
			ID:    "GDP",
			Title: "Gross Domestic Product",
			Tags:  []string{"gdp", "economic-growth"},
			Sentiment: func(val, prev float64) float64 {
				if val > prev {
					return 0.5 // GDP growth = positive
				}
				return -0.4
			},
			Urgency: signal.UrgencyMedium,
		},
		{
			ID:    "MORTGAGE30US",
			Title: "30-Year Fixed Rate Mortgage Average",
			Tags:  []string{"mortgage", "housing", "interest-rate"},
			Sentiment: func(val, prev float64) float64 {
				if val > prev {
					return -0.2 // Rising mortgage rates
				}
				return 0.2
			},
			Urgency: signal.UrgencyLow,
		},
		{
			ID:    "DTWEXBGS",
			Title: "Trade Weighted U.S. Dollar Index",
			Tags:  []string{"dollar-index", "currency", "forex"},
			Sentiment: func(val, prev float64) float64 {
				change := (val - prev) / prev * 100
				if change > 1 {
					return 0.2 // Strong dollar
				} else if change < -1 {
					return -0.2 // Weak dollar
				}
				return 0
			},
			Urgency: signal.UrgencyLow,
		},
		{
			ID:    "VIXCLS",
			Title: "CBOE Volatility Index: VIX",
			Tags:  []string{"vix", "volatility", "fear-index"},
			Sentiment: func(val, _ float64) float64 {
				if val > 30 {
					return -0.7 // High fear
				} else if val > 20 {
					return -0.3 // Elevated
				}
				return 0.2 // Low volatility
			},
			Urgency: signal.UrgencyMedium,
		},
	}
}

type Config struct {
	APIKey       string
	BaseURL      string
	PollInterval time.Duration
	Series       []SeriesMetadata
	Enabled      bool
}

func DefaultConfig() Config {
	return Config{
		BaseURL:      DefaultBaseURL,
		PollInterval: DefaultPollInterval,
		Series:       DefaultSeries(),
		Enabled:      true,
	}
}

type Provider struct {
	*provider.BaseProvider

	config     Config
	httpClient *http.Client

	mu         sync.RWMutex
	lastValues map[string]float64 // Track previous values for change detection

	wg sync.WaitGroup
}

func New(cfg Config, logger *zap.Logger) (*Provider, error) {
	if cfg.APIKey == "" {
		return nil, fmt.Errorf("FRED API key is required")
	}

	if logger == nil {
		logger = zap.NewNop()
	}

	baseCfg := provider.ProviderConfig{
		Name:           "fred",
		Category:       signal.CategoryMacro,
		PollInterval:   cfg.PollInterval,
		RequestTimeout: 30 * time.Second,
		MaxRetries:     3,
		RetryBackoff:   time.Second,
	}

	return &Provider{
		BaseProvider: provider.NewBaseProvider(baseCfg, logger.Named("fred")),
		config:       cfg,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		lastValues: make(map[string]float64),
	}, nil
}

func (p *Provider) Start(ctx context.Context) error {
	if !p.config.Enabled {
		p.Logger().Info("FRED provider is disabled")
		return nil
	}

	p.MarkStarted()

	p.wg.Add(1)
	go p.pollLoop(ctx)

	p.Logger().Info("FRED provider started",
		zap.Duration("poll_interval", p.config.PollInterval),
		zap.Int("series_count", len(p.config.Series)),
	)

	return nil
}

func (p *Provider) Stop() error {
	err := p.BaseProvider.Stop()
	p.wg.Wait()
	p.Logger().Info("FRED provider stopped")
	return err
}

func (p *Provider) pollLoop(ctx context.Context) {
	defer p.wg.Done()

	p.poll(ctx)

	ticker := time.NewTicker(p.config.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-p.StopChannel():
			return
		case <-ticker.C:
			p.poll(ctx)
		}
	}
}

func (p *Provider) poll(ctx context.Context) {
	p.Logger().Debug("Polling FRED for economic data")

	for _, series := range p.config.Series {
		if p.IsStopped() {
			return
		}

		sig, err := p.fetchSeries(ctx, series)
		if err != nil {
			p.Logger().Warn("Failed to fetch series",
				zap.String("series", series.ID),
				zap.Error(err),
			)
			p.RecordError(err)
			continue
		}

		if sig != nil {
			if err := p.Emit(ctx, *sig); err != nil {
				p.Logger().Warn("Failed to emit signal", zap.Error(err))
			}
		}
	}

	p.Logger().Debug("FRED poll complete", zap.Int("series_count", len(p.config.Series)))
}

type fredObservation struct {
	Date  string `json:"date"`
	Value string `json:"value"`
}

type fredResponse struct {
	Observations []fredObservation `json:"observations"`
}

func (p *Provider) fetchSeries(ctx context.Context, series SeriesMetadata) (*signal.Signal, error) {
	reqURL, err := url.Parse(fmt.Sprintf("%s/series/observations", p.config.BaseURL))
	if err != nil {
		return nil, err
	}

	q := reqURL.Query()
	q.Set("series_id", series.ID)
	q.Set("api_key", p.config.APIKey)
	q.Set("file_type", "json")
	q.Set("sort_order", "desc")
	q.Set("limit", "2") // Get latest 2 observations for change detection
	reqURL.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch series %s: %w", series.ID, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status for %s: %d", series.ID, resp.StatusCode)
	}

	var fredResp fredResponse
	if err := json.NewDecoder(resp.Body).Decode(&fredResp); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	if len(fredResp.Observations) == 0 {
		return nil, nil
	}

	latestObs := fredResp.Observations[0]
	if latestObs.Value == "." {
		return nil, nil // No data available
	}

	var value float64
	if _, err := fmt.Sscanf(latestObs.Value, "%f", &value); err != nil {
		return nil, fmt.Errorf("parse value: %w", err)
	}

	p.mu.RLock()
	prevValue, hasPrev := p.lastValues[series.ID]
	p.mu.RUnlock()

	if !hasPrev && len(fredResp.Observations) > 1 {
		if fredResp.Observations[1].Value != "." {
			fmt.Sscanf(fredResp.Observations[1].Value, "%f", &prevValue)
			hasPrev = true
		}
	}

	p.mu.Lock()
	p.lastValues[series.ID] = value
	p.mu.Unlock()

	obsDate, _ := time.Parse("2006-01-02", latestObs.Date)

	var sentiment float64
	if hasPrev && series.Sentiment != nil {
		sentiment = series.Sentiment(value, prevValue)
	}

	action := "data_release"
	if hasPrev {
		change := (value - prevValue) / prevValue * 100
		if change > 5 {
			action = "significant_increase"
		} else if change < -5 {
			action = "significant_decrease"
		}
	}

	builder := signal.NewBuilder(signal.SourceFRED, signal.CategoryMacro).
		WithTimestamp(obsDate).
		WithSubject(series.ID).
		WithAction(action).
		WithObject("US Economy").
		WithConfidence(0.95). // Official government data
		WithSentiment(sentiment).
		WithUrgency(series.Urgency).
		WithTags(series.Tags...).
		WithRawData(map[string]interface{}{
			"series_id":    series.ID,
			"series_title": series.Title,
			"value":        value,
			"previous":     prevValue,
			"date":         latestObs.Date,
			"has_previous": hasPrev,
		})

	sig, err := builder.Build()
	if err != nil {
		return nil, fmt.Errorf("build signal: %w", err)
	}

	return &sig, nil
}
