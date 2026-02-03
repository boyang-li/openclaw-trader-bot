// Package tradingeconomics provides a provider for Trading Economics economic calendar and data.
// It fetches economic indicators, events, and forecasts from the Trading Economics API.
package tradingeconomics

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/openclaworg/l1-ingestion/internal/provider"
	"github.com/openclaworg/l1-ingestion/internal/signal"
)

const (
	DefaultBaseURL      = "https://api.tradingeconomics.com"
	DefaultPollInterval = 5 * time.Minute
)

var DefaultCountries = []string{
	"united states", "china", "euro area", "japan", "germany",
	"united kingdom", "france", "india", "brazil", "russia",
}

var DefaultIndicators = []string{
	"interest rate", "inflation rate", "gdp growth rate",
	"unemployment rate", "balance of trade", "consumer confidence",
	"manufacturing pmi", "services pmi", "retail sales",
	"industrial production",
}

var HighImpactIndicators = map[string]bool{
	"interest rate":     true,
	"gdp growth rate":   true,
	"inflation rate":    true,
	"non farm payrolls": true,
	"unemployment rate": true,
	"fomc":              true,
	"ecb interest rate": true,
	"boj interest rate": true,
	"fed chair":         true,
}

type Config struct {
	APIKey       string
	BaseURL      string
	PollInterval time.Duration
	Countries    []string
	Indicators   []string
	Enabled      bool
}

func DefaultConfig() Config {
	return Config{
		BaseURL:      DefaultBaseURL,
		PollInterval: DefaultPollInterval,
		Countries:    DefaultCountries,
		Indicators:   DefaultIndicators,
		Enabled:      true,
	}
}

type Provider struct {
	*provider.BaseProvider

	config     Config
	httpClient *http.Client

	mu         sync.RWMutex
	seenEvents map[string]time.Time
	lastPoll   time.Time

	wg sync.WaitGroup
}

type CalendarEvent struct {
	ID         string    `json:"CalendarId"`
	Date       string    `json:"Date"`
	Country    string    `json:"Country"`
	Category   string    `json:"Category"`
	Event      string    `json:"Event"`
	Reference  string    `json:"Reference"`
	Source     string    `json:"Source"`
	Actual     *float64  `json:"Actual"`
	Previous   *float64  `json:"Previous"`
	Forecast   *float64  `json:"Forecast"`
	TEForecast *float64  `json:"TEForecast"`
	Importance int       `json:"Importance"`
	Currency   string    `json:"Currency"`
	Unit       string    `json:"Unit"`
	Ticker     string    `json:"Ticker"`
	URL        string    `json:"URL"`
	DateSpan   string    `json:"DateSpan"`
	LastUpdate time.Time `json:"-"`
}

type Indicator struct {
	Country           string   `json:"Country"`
	Category          string   `json:"Category"`
	Title             string   `json:"Title"`
	LatestValue       *float64 `json:"LatestValue"`
	LatestValueDate   string   `json:"LatestValueDate"`
	Source            string   `json:"Source"`
	Unit              string   `json:"Unit"`
	URL               string   `json:"URL"`
	CategoryGroup     string   `json:"CategoryGroup"`
	Frequency         string   `json:"Frequency"`
	PreviousValue     *float64 `json:"PreviousValue"`
	PreviousValueDate string   `json:"PreviousValueDate"`
}

func New(cfg Config, logger *zap.Logger) (*Provider, error) {
	if cfg.APIKey == "" {
		return nil, fmt.Errorf("trading economics: API key is required")
	}

	if logger == nil {
		logger = zap.NewNop()
	}

	if cfg.BaseURL == "" {
		cfg.BaseURL = DefaultBaseURL
	}
	if cfg.PollInterval == 0 {
		cfg.PollInterval = DefaultPollInterval
	}
	if len(cfg.Countries) == 0 {
		cfg.Countries = DefaultCountries
	}

	baseCfg := provider.ProviderConfig{
		Name:           "trading_economics",
		Category:       signal.CategoryMacro,
		PollInterval:   cfg.PollInterval,
		RequestTimeout: 30 * time.Second,
		MaxRetries:     3,
		RetryBackoff:   10 * time.Second,
	}

	return &Provider{
		BaseProvider: provider.NewBaseProvider(baseCfg, logger.Named("trading_economics")),
		config:       cfg,
		httpClient:   &http.Client{Timeout: 30 * time.Second},
		seenEvents:   make(map[string]time.Time),
	}, nil
}

func (p *Provider) Start(ctx context.Context) error {
	if !p.config.Enabled {
		p.Logger().Info("Trading Economics provider is disabled")
		return nil
	}

	p.MarkStarted()

	p.wg.Add(1)
	go p.pollLoop(ctx)

	p.Logger().Info("Trading Economics provider started",
		zap.Duration("poll_interval", p.config.PollInterval),
		zap.Strings("countries", p.config.Countries),
	)

	return nil
}

func (p *Provider) Stop() error {
	err := p.BaseProvider.Stop()
	p.wg.Wait()
	p.Logger().Info("Trading Economics provider stopped")
	return err
}

func (p *Provider) pollLoop(ctx context.Context) {
	defer p.wg.Done()

	ticker := time.NewTicker(p.config.PollInterval)
	defer ticker.Stop()

	if err := p.poll(ctx); err != nil {
		p.Logger().Warn("Initial poll failed", zap.Error(err))
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-p.StopChannel():
			return
		case <-ticker.C:
			if err := p.poll(ctx); err != nil {
				p.Logger().Warn("Poll failed", zap.Error(err))
				p.RecordError(err)
			}
		}
	}
}

func (p *Provider) poll(ctx context.Context) error {
	events, err := p.fetchCalendar(ctx)
	if err != nil {
		return fmt.Errorf("fetch calendar: %w", err)
	}

	for _, event := range events {
		eventKey := fmt.Sprintf("%s_%s_%s", event.Country, event.Event, event.Date)

		p.mu.Lock()
		_, seen := p.seenEvents[eventKey]
		hasActual := event.Actual != nil
		if !seen || hasActual {
			p.seenEvents[eventKey] = time.Now()
		}
		p.mu.Unlock()

		if hasActual && !seen {
			if err := p.emitEventSignal(ctx, event); err != nil {
				p.Logger().Warn("Failed to emit signal",
					zap.Error(err),
					zap.String("event", event.Event),
				)
			}
		}
	}

	p.cleanupSeenEvents()

	return nil
}

func (p *Provider) fetchCalendar(ctx context.Context) ([]CalendarEvent, error) {
	params := url.Values{}
	params.Set("c", p.config.APIKey)

	countries := strings.Join(p.config.Countries, ",")
	if countries != "" {
		params.Set("country", countries)
	}

	today := time.Now().Format("2006-01-02")
	tomorrow := time.Now().Add(24 * time.Hour).Format("2006-01-02")

	reqURL := fmt.Sprintf("%s/calendar/country/%s/%s/%s?%s",
		p.config.BaseURL,
		url.PathEscape(countries),
		today,
		tomorrow,
		params.Encode(),
	)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "ACC-L1-Ingestion/1.0")

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("API error: HTTP %d", resp.StatusCode)
	}

	var events []CalendarEvent
	if err := json.NewDecoder(resp.Body).Decode(&events); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	return events, nil
}

func (p *Provider) emitEventSignal(ctx context.Context, event CalendarEvent) error {
	action := determineEventAction(event)
	sentiment := calculateEventSentiment(event)
	urgency := calculateEventUrgency(event)

	tags := generateEventTags(event)

	eventTime, _ := time.Parse("2006-01-02T15:04:05", event.Date)
	if eventTime.IsZero() {
		eventTime, _ = time.Parse("2006-01-02", event.Date)
	}
	if eventTime.IsZero() {
		eventTime = time.Now()
	}

	object := formatEventResult(event)

	builder := signal.NewBuilder(signal.SourceTradingEcon, signal.CategoryMacro).
		WithTimestamp(eventTime).
		WithSubject(event.Country).
		WithAction(action).
		WithObject(object).
		WithConfidence(0.95).
		WithSentiment(sentiment).
		WithUrgency(urgency).
		WithTags(tags...).
		WithProviderMeta(map[string]any{
			"source_id":  event.ID,
			"source_url": event.URL,
		}).
		WithRawData(map[string]interface{}{
			"calendar_id": event.ID,
			"country":     event.Country,
			"category":    event.Category,
			"event":       event.Event,
			"reference":   event.Reference,
			"actual":      event.Actual,
			"previous":    event.Previous,
			"forecast":    event.Forecast,
			"te_forecast": event.TEForecast,
			"importance":  event.Importance,
			"unit":        event.Unit,
			"currency":    event.Currency,
		})

	sig, err := builder.Build()
	if err != nil {
		return fmt.Errorf("build signal: %w", err)
	}

	if err := p.Emit(ctx, sig); err != nil {
		return fmt.Errorf("emit signal: %w", err)
	}

	p.Logger().Info("Economic event processed",
		zap.String("country", event.Country),
		zap.String("event", event.Event),
		zap.Float64p("actual", event.Actual),
		zap.Float64p("forecast", event.Forecast),
		zap.Int("importance", event.Importance),
	)

	return nil
}

func determineEventAction(event CalendarEvent) string {
	category := strings.ToLower(event.Category)
	eventName := strings.ToLower(event.Event)

	switch {
	case strings.Contains(category, "interest rate") || strings.Contains(eventName, "rate decision"):
		return "rate_decision"
	case strings.Contains(category, "gdp"):
		return "gdp_release"
	case strings.Contains(category, "inflation") || strings.Contains(category, "cpi"):
		return "inflation_release"
	case strings.Contains(category, "employment") || strings.Contains(eventName, "payroll"):
		return "employment_release"
	case strings.Contains(category, "trade") || strings.Contains(category, "balance"):
		return "trade_release"
	case strings.Contains(category, "pmi") || strings.Contains(eventName, "pmi"):
		return "pmi_release"
	case strings.Contains(eventName, "speech") || strings.Contains(eventName, "speaks"):
		return "central_bank_speech"
	case strings.Contains(eventName, "minutes"):
		return "minutes_release"
	default:
		return "economic_release"
	}
}

func calculateEventSentiment(event CalendarEvent) float64 {
	if event.Actual == nil || event.Forecast == nil {
		return 0
	}

	actual := *event.Actual
	forecast := *event.Forecast

	if forecast == 0 {
		return 0
	}

	surprise := (actual - forecast) / absFloat(forecast)

	category := strings.ToLower(event.Category)
	isNegativeGood := strings.Contains(category, "unemployment") ||
		strings.Contains(category, "inflation") ||
		strings.Contains(category, "jobless")

	sentiment := surprise
	if isNegativeGood {
		sentiment = -surprise
	}

	if sentiment > 1.0 {
		sentiment = 1.0
	} else if sentiment < -1.0 {
		sentiment = -1.0
	}

	return sentiment
}

func absFloat(f float64) float64 {
	if f < 0 {
		return -f
	}
	return f
}

func calculateEventUrgency(event CalendarEvent) signal.UrgencyLevel {
	eventName := strings.ToLower(event.Event)
	isHighImpact := HighImpactIndicators[strings.ToLower(event.Category)]

	for key := range HighImpactIndicators {
		if strings.Contains(eventName, key) {
			isHighImpact = true
			break
		}
	}

	switch {
	case event.Importance >= 3 || isHighImpact:
		return signal.UrgencyHigh
	case event.Importance == 2:
		return signal.UrgencyMedium
	default:
		return signal.UrgencyLow
	}
}

func formatEventResult(event CalendarEvent) string {
	if event.Actual == nil {
		return fmt.Sprintf("%s (pending)", event.Event)
	}

	actual := *event.Actual
	unit := event.Unit
	if unit == "" {
		unit = ""
	} else {
		unit = " " + unit
	}

	result := fmt.Sprintf("%s: %.2f%s", event.Event, actual, unit)

	if event.Forecast != nil {
		forecast := *event.Forecast
		if actual > forecast {
			result += " (beat)"
		} else if actual < forecast {
			result += " (miss)"
		} else {
			result += " (inline)"
		}
	}

	return result
}

func generateEventTags(event CalendarEvent) []string {
	tags := []string{"economic-calendar", "macro"}

	country := strings.ToLower(strings.ReplaceAll(event.Country, " ", "-"))
	tags = append(tags, country)

	category := strings.ToLower(strings.ReplaceAll(event.Category, " ", "-"))
	if category != "" {
		tags = append(tags, category)
	}

	switch {
	case strings.Contains(category, "interest") || strings.Contains(category, "rate"):
		tags = append(tags, "central-bank", "rates")
	case strings.Contains(category, "gdp"):
		tags = append(tags, "growth")
	case strings.Contains(category, "inflation") || strings.Contains(category, "cpi"):
		tags = append(tags, "inflation")
	case strings.Contains(category, "employ") || strings.Contains(category, "job"):
		tags = append(tags, "labor-market")
	case strings.Contains(category, "pmi"):
		tags = append(tags, "survey")
	case strings.Contains(category, "trade"):
		tags = append(tags, "trade")
	}

	switch event.Importance {
	case 3:
		tags = append(tags, "high-impact")
	case 2:
		tags = append(tags, "medium-impact")
	case 1:
		tags = append(tags, "low-impact")
	}

	if event.Actual != nil && event.Forecast != nil {
		if *event.Actual > *event.Forecast {
			tags = append(tags, "beat")
		} else if *event.Actual < *event.Forecast {
			tags = append(tags, "miss")
		}
	}

	return tags
}

func (p *Provider) cleanupSeenEvents() {
	p.mu.Lock()
	defer p.mu.Unlock()

	cutoff := time.Now().Add(-24 * time.Hour)
	for key, seenAt := range p.seenEvents {
		if seenAt.Before(cutoff) {
			delete(p.seenEvents, key)
		}
	}
}
