// Package binance provides a provider for Binance cryptocurrency exchange data.
// It connects to Binance WebSocket streams for real-time market data.
package binance

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"go.uber.org/zap"

	"github.com/openclaworg/l1-ingestion/internal/provider"
	"github.com/openclaworg/l1-ingestion/internal/signal"
)

const (
	DefaultWSBaseURL   = "wss://stream.binance.com:9443"
	DefaultRESTBaseURL = "https://api.binance.com"
	DefaultPingPeriod  = 30 * time.Second
	ReconnectDelay     = 5 * time.Second
)

type Config struct {
	WSBaseURL               string
	RESTBaseURL             string
	Pairs                   []string
	LargeTradeThresholdUSD  float64
	PriceChangeThresholdPct float64
	VolumeSpikeMultiplier   float64
	Enabled                 bool
}

func DefaultConfig() Config {
	return Config{
		WSBaseURL:               DefaultWSBaseURL,
		RESTBaseURL:             DefaultRESTBaseURL,
		Pairs:                   []string{"BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT"},
		LargeTradeThresholdUSD:  1_000_000,
		PriceChangeThresholdPct: 2.0,
		VolumeSpikeMultiplier:   3.0,
		Enabled:                 true,
	}
}

type Provider struct {
	*provider.BaseProvider

	config     Config
	httpClient *http.Client

	mu            sync.RWMutex
	conn          *websocket.Conn
	prices        map[string]float64
	volumes24h    map[string]float64
	priceChanges  map[string]float64
	lastTradeTime map[string]time.Time

	wg sync.WaitGroup
}

func New(cfg Config, logger *zap.Logger) *Provider {
	if logger == nil {
		logger = zap.NewNop()
	}

	baseCfg := provider.ProviderConfig{
		Name:           "binance",
		Category:       signal.CategoryCrypto,
		PollInterval:   time.Second,
		RequestTimeout: 10 * time.Second,
		MaxRetries:     3,
		RetryBackoff:   time.Second,
	}

	return &Provider{
		BaseProvider: provider.NewBaseProvider(baseCfg, logger.Named("binance")),
		config:       cfg,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
		prices:        make(map[string]float64),
		volumes24h:    make(map[string]float64),
		priceChanges:  make(map[string]float64),
		lastTradeTime: make(map[string]time.Time),
	}
}

func (p *Provider) Start(ctx context.Context) error {
	if !p.config.Enabled {
		p.Logger().Info("Binance provider is disabled")
		return nil
	}

	if err := p.fetchInitialPrices(ctx); err != nil {
		p.Logger().Warn("Failed to fetch initial prices", zap.Error(err))
	}

	p.MarkStarted()

	p.wg.Add(1)
	go p.wsLoop(ctx)

	p.Logger().Info("Binance provider started",
		zap.Strings("pairs", p.config.Pairs),
		zap.Float64("large_trade_threshold", p.config.LargeTradeThresholdUSD),
	)

	return nil
}

func (p *Provider) Stop() error {
	err := p.BaseProvider.Stop()

	p.mu.Lock()
	if p.conn != nil {
		p.conn.Close()
	}
	p.mu.Unlock()

	p.wg.Wait()
	p.Logger().Info("Binance provider stopped")
	return err
}

func (p *Provider) fetchInitialPrices(ctx context.Context) error {
	url := fmt.Sprintf("%s/api/v3/ticker/24hr", p.config.RESTBaseURL)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	var tickers []struct {
		Symbol             string `json:"symbol"`
		LastPrice          string `json:"lastPrice"`
		PriceChangePercent string `json:"priceChangePercent"`
		Volume             string `json:"volume"`
		QuoteVolume        string `json:"quoteVolume"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&tickers); err != nil {
		return err
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	pairSet := make(map[string]bool)
	for _, pair := range p.config.Pairs {
		pairSet[strings.ToUpper(pair)] = true
	}

	for _, t := range tickers {
		if !pairSet[t.Symbol] {
			continue
		}

		var price, change, volume float64
		fmt.Sscanf(t.LastPrice, "%f", &price)
		fmt.Sscanf(t.PriceChangePercent, "%f", &change)
		fmt.Sscanf(t.QuoteVolume, "%f", &volume)

		p.prices[t.Symbol] = price
		p.priceChanges[t.Symbol] = change
		p.volumes24h[t.Symbol] = volume
	}

	return nil
}

func (p *Provider) wsLoop(ctx context.Context) {
	defer p.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case <-p.StopChannel():
			return
		default:
		}

		if err := p.connectAndListen(ctx); err != nil {
			p.Logger().Warn("WebSocket error", zap.Error(err))
			p.RecordError(err)
		}

		select {
		case <-ctx.Done():
			return
		case <-p.StopChannel():
			return
		case <-time.After(ReconnectDelay):
			p.Logger().Info("Reconnecting to Binance WebSocket")
		}
	}
}

func (p *Provider) connectAndListen(ctx context.Context) error {
	streams := make([]string, 0, len(p.config.Pairs)*2)
	for _, pair := range p.config.Pairs {
		symbol := strings.ToLower(pair)
		streams = append(streams,
			fmt.Sprintf("%s@trade", symbol),
			fmt.Sprintf("%s@ticker", symbol),
		)
	}

	wsURL := fmt.Sprintf("%s/stream?streams=%s", p.config.WSBaseURL, strings.Join(streams, "/"))

	dialer := websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
	}

	conn, _, err := dialer.DialContext(ctx, wsURL, nil)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}

	p.mu.Lock()
	p.conn = conn
	p.mu.Unlock()

	defer func() {
		p.mu.Lock()
		if p.conn == conn {
			p.conn = nil
		}
		p.mu.Unlock()
		conn.Close()
	}()

	p.Logger().Debug("Connected to Binance WebSocket", zap.Int("streams", len(streams)))

	pingTicker := time.NewTicker(DefaultPingPeriod)
	defer pingTicker.Stop()

	errCh := make(chan error, 1)
	go func() {
		for {
			var msg wsMessage
			if err := conn.ReadJSON(&msg); err != nil {
				errCh <- err
				return
			}
			p.handleMessage(ctx, msg)
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-p.StopChannel():
			return nil
		case err := <-errCh:
			return err
		case <-pingTicker.C:
			if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return fmt.Errorf("ping: %w", err)
			}
		}
	}
}

type wsMessage struct {
	Stream string          `json:"stream"`
	Data   json.RawMessage `json:"data"`
}

type tradeData struct {
	Symbol    string `json:"s"`
	Price     string `json:"p"`
	Quantity  string `json:"q"`
	TradeTime int64  `json:"T"`
	IsBuyer   bool   `json:"m"`
}

type tickerData struct {
	Symbol             string `json:"s"`
	PriceChange        string `json:"p"`
	PriceChangePercent string `json:"P"`
	LastPrice          string `json:"c"`
	Volume             string `json:"v"`
	QuoteVolume        string `json:"q"`
}

func (p *Provider) handleMessage(ctx context.Context, msg wsMessage) {
	parts := strings.Split(msg.Stream, "@")
	if len(parts) != 2 {
		return
	}

	streamType := parts[1]

	switch streamType {
	case "trade":
		p.handleTrade(ctx, msg.Data)
	case "ticker":
		p.handleTicker(ctx, msg.Data)
	}
}

func (p *Provider) handleTrade(ctx context.Context, data json.RawMessage) {
	var trade tradeData
	if err := json.Unmarshal(data, &trade); err != nil {
		return
	}

	var price, qty float64
	fmt.Sscanf(trade.Price, "%f", &price)
	fmt.Sscanf(trade.Quantity, "%f", &qty)

	tradeValue := price * qty

	if tradeValue >= p.config.LargeTradeThresholdUSD {
		p.emitLargeTradeSignal(ctx, trade, price, qty, tradeValue)
	}

	p.mu.Lock()
	p.prices[trade.Symbol] = price
	p.lastTradeTime[trade.Symbol] = time.Now()
	p.mu.Unlock()
}

func (p *Provider) handleTicker(ctx context.Context, data json.RawMessage) {
	var ticker tickerData
	if err := json.Unmarshal(data, &ticker); err != nil {
		return
	}

	var price, change, volume float64
	fmt.Sscanf(ticker.LastPrice, "%f", &price)
	fmt.Sscanf(ticker.PriceChangePercent, "%f", &change)
	fmt.Sscanf(ticker.QuoteVolume, "%f", &volume)

	p.mu.Lock()
	prevChange := p.priceChanges[ticker.Symbol]
	p.prices[ticker.Symbol] = price
	p.priceChanges[ticker.Symbol] = change
	p.volumes24h[ticker.Symbol] = volume
	p.mu.Unlock()

	if math.Abs(change) >= p.config.PriceChangeThresholdPct {
		crossedThreshold := math.Abs(prevChange) < p.config.PriceChangeThresholdPct
		if crossedThreshold {
			p.emitPriceMovementSignal(ctx, ticker, price, change)
		}
	}
}

func (p *Provider) emitLargeTradeSignal(ctx context.Context, trade tradeData, price, qty, value float64) {
	direction := "buy"
	sentiment := 0.3
	if trade.IsBuyer {
		direction = "sell"
		sentiment = -0.3
	}

	action := fmt.Sprintf("large_%s", direction)

	builder := signal.NewBuilder(signal.SourceBinance, signal.CategoryCrypto).
		WithTimestamp(time.UnixMilli(trade.TradeTime)).
		WithSubject(trade.Symbol).
		WithAction(action).
		WithObject("Spot Market").
		WithConfidence(0.99).
		WithSentiment(sentiment).
		WithUrgency(signal.UrgencyHigh).
		WithTags("large-trade", "spot", strings.ToLower(trade.Symbol[:3])).
		WithRawData(map[string]interface{}{
			"price":      price,
			"quantity":   qty,
			"value_usd":  value,
			"direction":  direction,
			"trade_time": trade.TradeTime,
		})

	sig, err := builder.Build()
	if err != nil {
		p.Logger().Warn("Failed to build large trade signal", zap.Error(err))
		return
	}

	if err := p.Emit(ctx, sig); err != nil {
		p.Logger().Warn("Failed to emit large trade signal", zap.Error(err))
	} else {
		p.Logger().Info("Large trade detected",
			zap.String("symbol", trade.Symbol),
			zap.Float64("value_usd", value),
			zap.String("direction", direction),
		)
	}
}

func (p *Provider) emitPriceMovementSignal(ctx context.Context, ticker tickerData, price, change float64) {
	action := "significant_move_up"
	sentiment := 0.5
	if change < 0 {
		action = "significant_move_down"
		sentiment = -0.5
	}

	builder := signal.NewBuilder(signal.SourceBinance, signal.CategoryCrypto).
		WithTimestamp(time.Now()).
		WithSubject(ticker.Symbol).
		WithAction(action).
		WithObject("Spot Market").
		WithConfidence(0.99).
		WithSentiment(sentiment).
		WithUrgency(signal.UrgencyMedium).
		WithTags("price-movement", "spot", strings.ToLower(ticker.Symbol[:3])).
		WithRawData(map[string]interface{}{
			"price":          price,
			"change_24h_pct": change,
		})

	sig, err := builder.Build()
	if err != nil {
		p.Logger().Warn("Failed to build price movement signal", zap.Error(err))
		return
	}

	if err := p.Emit(ctx, sig); err != nil {
		p.Logger().Warn("Failed to emit price movement signal", zap.Error(err))
	} else {
		p.Logger().Info("Significant price movement",
			zap.String("symbol", ticker.Symbol),
			zap.Float64("change_pct", change),
		)
	}
}
