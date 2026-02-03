package binance

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/openclaworg/l1-ingestion/internal/signal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestNew(t *testing.T) {
	cfg := DefaultConfig()
	provider := New(cfg, nil)

	assert.NotNil(t, provider)
	assert.Equal(t, "binance", provider.Name())
	assert.Equal(t, signal.CategoryCrypto, provider.Category())
}

func TestNew_WithLogger(t *testing.T) {
	cfg := DefaultConfig()
	logger := zap.NewNop()
	provider := New(cfg, logger)

	assert.NotNil(t, provider)
	assert.Equal(t, "binance", provider.Name())
}

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	assert.Equal(t, DefaultWSBaseURL, cfg.WSBaseURL)
	assert.Equal(t, DefaultRESTBaseURL, cfg.RESTBaseURL)
	assert.Equal(t, []string{"BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT"}, cfg.Pairs)
	assert.Equal(t, float64(1_000_000), cfg.LargeTradeThresholdUSD)
	assert.Equal(t, 2.0, cfg.PriceChangeThresholdPct)
	assert.Equal(t, 3.0, cfg.VolumeSpikeMultiplier)
	assert.True(t, cfg.Enabled)
}

func TestHandleTrade(t *testing.T) {
	tests := []struct {
		name         string
		trade        tradeData
		threshold    float64
		expectSignal bool
		expectBuy    bool
	}{
		{
			name: "large buy trade above threshold",
			trade: tradeData{
				Symbol:    "BTCUSDT",
				Price:     "50000.00",
				Quantity:  "25.0",
				TradeTime: time.Now().UnixMilli(),
				IsBuyer:   false,
			},
			threshold:    1_000_000,
			expectSignal: true,
			expectBuy:    true,
		},
		{
			name: "large sell trade above threshold",
			trade: tradeData{
				Symbol:    "BTCUSDT",
				Price:     "50000.00",
				Quantity:  "25.0",
				TradeTime: time.Now().UnixMilli(),
				IsBuyer:   true,
			},
			threshold:    1_000_000,
			expectSignal: true,
			expectBuy:    false,
		},
		{
			name: "small trade below threshold",
			trade: tradeData{
				Symbol:    "BTCUSDT",
				Price:     "50000.00",
				Quantity:  "0.01",
				TradeTime: time.Now().UnixMilli(),
				IsBuyer:   false,
			},
			threshold:    1_000_000,
			expectSignal: false,
		},
		{
			name: "trade exactly at threshold",
			trade: tradeData{
				Symbol:    "BTCUSDT",
				Price:     "50000.00",
				Quantity:  "20.0",
				TradeTime: time.Now().UnixMilli(),
				IsBuyer:   false,
			},
			threshold:    1_000_000,
			expectSignal: true,
			expectBuy:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.LargeTradeThresholdUSD = tt.threshold
			provider := New(cfg, zap.NewNop())

			var emittedSignals []signal.Signal
			var mu sync.Mutex
			provider.Subscribe(func(ctx context.Context, sig signal.Signal) error {
				mu.Lock()
				emittedSignals = append(emittedSignals, sig)
				mu.Unlock()
				return nil
			})

			data, err := json.Marshal(tt.trade)
			require.NoError(t, err)

			ctx := context.Background()
			provider.handleTrade(ctx, data)

			mu.Lock()
			defer mu.Unlock()

			if tt.expectSignal {
				require.Len(t, emittedSignals, 1, "expected 1 signal to be emitted")
				sig := emittedSignals[0]
				assert.Equal(t, signal.SourceBinance, sig.Source)
				assert.Equal(t, signal.CategoryCrypto, sig.Category)
				assert.Equal(t, tt.trade.Symbol, sig.Subject)
				assert.Equal(t, signal.UrgencyHigh, sig.Urgency)

				if tt.expectBuy {
					assert.Equal(t, "large_buy", sig.Action)
					assert.True(t, sig.Sentiment > 0, "buy should have positive sentiment")
				} else {
					assert.Equal(t, "large_sell", sig.Action)
					assert.True(t, sig.Sentiment < 0, "sell should have negative sentiment")
				}
			} else {
				assert.Len(t, emittedSignals, 0, "expected no signals to be emitted")
			}

			provider.mu.RLock()
			defer provider.mu.RUnlock()
			if tt.trade.Price != "" {
				assert.NotZero(t, provider.prices[tt.trade.Symbol])
			}
		})
	}
}

func TestHandleTicker(t *testing.T) {
	tests := []struct {
		name         string
		ticker       tickerData
		prevChange   float64
		threshold    float64
		expectSignal bool
		expectUp     bool
	}{
		{
			name: "significant move up crossing threshold",
			ticker: tickerData{
				Symbol:             "BTCUSDT",
				PriceChange:        "1000.00",
				PriceChangePercent: "3.5",
				LastPrice:          "51000.00",
				Volume:             "1000",
				QuoteVolume:        "50000000",
			},
			prevChange:   1.0,
			threshold:    2.0,
			expectSignal: true,
			expectUp:     true,
		},
		{
			name: "significant move down crossing threshold",
			ticker: tickerData{
				Symbol:             "BTCUSDT",
				PriceChange:        "-1000.00",
				PriceChangePercent: "-3.5",
				LastPrice:          "49000.00",
				Volume:             "1000",
				QuoteVolume:        "50000000",
			},
			prevChange:   -1.0,
			threshold:    2.0,
			expectSignal: true,
			expectUp:     false,
		},
		{
			name: "already above threshold no signal",
			ticker: tickerData{
				Symbol:             "BTCUSDT",
				PriceChange:        "1000.00",
				PriceChangePercent: "3.5",
				LastPrice:          "51000.00",
				Volume:             "1000",
				QuoteVolume:        "50000000",
			},
			prevChange:   3.0,
			threshold:    2.0,
			expectSignal: false,
		},
		{
			name: "below threshold no signal",
			ticker: tickerData{
				Symbol:             "BTCUSDT",
				PriceChange:        "500.00",
				PriceChangePercent: "1.0",
				LastPrice:          "50500.00",
				Volume:             "1000",
				QuoteVolume:        "50000000",
			},
			prevChange:   0.5,
			threshold:    2.0,
			expectSignal: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.PriceChangeThresholdPct = tt.threshold
			provider := New(cfg, zap.NewNop())

			provider.mu.Lock()
			provider.priceChanges[tt.ticker.Symbol] = tt.prevChange
			provider.mu.Unlock()

			var emittedSignals []signal.Signal
			var mu sync.Mutex
			provider.Subscribe(func(ctx context.Context, sig signal.Signal) error {
				mu.Lock()
				emittedSignals = append(emittedSignals, sig)
				mu.Unlock()
				return nil
			})

			data, err := json.Marshal(tt.ticker)
			require.NoError(t, err)

			ctx := context.Background()
			provider.handleTicker(ctx, data)

			mu.Lock()
			defer mu.Unlock()

			if tt.expectSignal {
				require.Len(t, emittedSignals, 1, "expected 1 signal to be emitted")
				sig := emittedSignals[0]
				assert.Equal(t, signal.SourceBinance, sig.Source)
				assert.Equal(t, signal.CategoryCrypto, sig.Category)
				assert.Equal(t, tt.ticker.Symbol, sig.Subject)
				assert.Equal(t, signal.UrgencyMedium, sig.Urgency)

				if tt.expectUp {
					assert.Equal(t, "significant_move_up", sig.Action)
					assert.True(t, sig.Sentiment > 0, "up move should have positive sentiment")
				} else {
					assert.Equal(t, "significant_move_down", sig.Action)
					assert.True(t, sig.Sentiment < 0, "down move should have negative sentiment")
				}
			} else {
				assert.Len(t, emittedSignals, 0, "expected no signals to be emitted")
			}

			provider.mu.RLock()
			defer provider.mu.RUnlock()
			assert.NotZero(t, provider.prices[tt.ticker.Symbol])
			assert.NotZero(t, provider.volumes24h[tt.ticker.Symbol])
		})
	}
}

func TestHandleMessage(t *testing.T) {
	cfg := DefaultConfig()
	provider := New(cfg, zap.NewNop())

	var emittedSignals []signal.Signal
	var mu sync.Mutex
	provider.Subscribe(func(ctx context.Context, sig signal.Signal) error {
		mu.Lock()
		emittedSignals = append(emittedSignals, sig)
		mu.Unlock()
		return nil
	})

	ctx := context.Background()

	t.Run("trade stream", func(t *testing.T) {
		trade := tradeData{
			Symbol:    "BTCUSDT",
			Price:     "50000.00",
			Quantity:  "0.01",
			TradeTime: time.Now().UnixMilli(),
			IsBuyer:   false,
		}
		data, _ := json.Marshal(trade)

		msg := wsMessage{
			Stream: "btcusdt@trade",
			Data:   data,
		}

		provider.handleMessage(ctx, msg)

		provider.mu.RLock()
		assert.NotZero(t, provider.prices["BTCUSDT"])
		provider.mu.RUnlock()
	})

	t.Run("ticker stream", func(t *testing.T) {
		ticker := tickerData{
			Symbol:             "ETHUSDT",
			PriceChange:        "100.00",
			PriceChangePercent: "1.0",
			LastPrice:          "3000.00",
			Volume:             "1000",
			QuoteVolume:        "3000000",
		}
		data, _ := json.Marshal(ticker)

		msg := wsMessage{
			Stream: "ethusdt@ticker",
			Data:   data,
		}

		provider.handleMessage(ctx, msg)

		provider.mu.RLock()
		assert.NotZero(t, provider.prices["ETHUSDT"])
		provider.mu.RUnlock()
	})

	t.Run("invalid stream format", func(t *testing.T) {
		msg := wsMessage{
			Stream: "invalid",
			Data:   json.RawMessage(`{}`),
		}

		provider.handleMessage(ctx, msg)
	})
}

func TestEmitLargeTradeSignal(t *testing.T) {
	cfg := DefaultConfig()
	provider := New(cfg, zap.NewNop())

	var emittedSignal signal.Signal
	provider.Subscribe(func(ctx context.Context, sig signal.Signal) error {
		emittedSignal = sig
		return nil
	})

	trade := tradeData{
		Symbol:    "BTCUSDT",
		Price:     "50000.00",
		Quantity:  "30.0",
		TradeTime: time.Date(2024, 1, 15, 12, 0, 0, 0, time.UTC).UnixMilli(),
		IsBuyer:   false,
	}

	ctx := context.Background()
	provider.emitLargeTradeSignal(ctx, trade, 50000.0, 30.0, 1500000.0)

	assert.Equal(t, signal.SourceBinance, emittedSignal.Source)
	assert.Equal(t, signal.CategoryCrypto, emittedSignal.Category)
	assert.Equal(t, "BTCUSDT", emittedSignal.Subject)
	assert.Equal(t, "large_buy", emittedSignal.Action)
	assert.Equal(t, "Spot Market", emittedSignal.Object)
	assert.Equal(t, 0.99, emittedSignal.Confidence)
	assert.Equal(t, 0.3, emittedSignal.Sentiment)
	assert.Equal(t, signal.UrgencyHigh, emittedSignal.Urgency)

	var rawData map[string]interface{}
	err := json.Unmarshal(emittedSignal.RawData, &rawData)
	require.NoError(t, err)
	assert.Equal(t, 50000.0, rawData["price"])
	assert.Equal(t, 30.0, rawData["quantity"])
	assert.Equal(t, 1500000.0, rawData["value_usd"])
	assert.Equal(t, "buy", rawData["direction"])
}

func TestEmitPriceMovementSignal(t *testing.T) {
	cfg := DefaultConfig()
	provider := New(cfg, zap.NewNop())

	var emittedSignal signal.Signal
	provider.Subscribe(func(ctx context.Context, sig signal.Signal) error {
		emittedSignal = sig
		return nil
	})

	t.Run("price move up", func(t *testing.T) {
		ticker := tickerData{
			Symbol:             "ETHUSDT",
			PriceChange:        "100.00",
			PriceChangePercent: "3.5",
			LastPrice:          "3100.00",
			Volume:             "1000",
			QuoteVolume:        "3100000",
		}

		ctx := context.Background()
		provider.emitPriceMovementSignal(ctx, ticker, 3100.0, 3.5)

		assert.Equal(t, signal.SourceBinance, emittedSignal.Source)
		assert.Equal(t, signal.CategoryCrypto, emittedSignal.Category)
		assert.Equal(t, "ETHUSDT", emittedSignal.Subject)
		assert.Equal(t, "significant_move_up", emittedSignal.Action)
		assert.Equal(t, 0.5, emittedSignal.Sentiment)
		assert.Equal(t, signal.UrgencyMedium, emittedSignal.Urgency)
	})

	t.Run("price move down", func(t *testing.T) {
		ticker := tickerData{
			Symbol:             "ETHUSDT",
			PriceChange:        "-100.00",
			PriceChangePercent: "-3.5",
			LastPrice:          "2900.00",
			Volume:             "1000",
			QuoteVolume:        "2900000",
		}

		ctx := context.Background()
		provider.emitPriceMovementSignal(ctx, ticker, 2900.0, -3.5)

		assert.Equal(t, "significant_move_down", emittedSignal.Action)
		assert.Equal(t, -0.5, emittedSignal.Sentiment)
	})
}

func TestFetchInitialPrices(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/api/v3/ticker/24hr", r.URL.Path)

		response := []map[string]string{
			{
				"symbol":             "BTCUSDT",
				"lastPrice":          "50000.00",
				"priceChangePercent": "2.5",
				"volume":             "10000",
				"quoteVolume":        "500000000",
			},
			{
				"symbol":             "ETHUSDT",
				"lastPrice":          "3000.00",
				"priceChangePercent": "-1.5",
				"volume":             "50000",
				"quoteVolume":        "150000000",
			},
			{
				"symbol":             "XRPUSDT",
				"lastPrice":          "0.50",
				"priceChangePercent": "0.5",
				"volume":             "1000000",
				"quoteVolume":        "500000",
			},
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	cfg := DefaultConfig()
	cfg.RESTBaseURL = server.URL
	cfg.Pairs = []string{"BTCUSDT", "ETHUSDT"}
	provider := New(cfg, zap.NewNop())

	ctx := context.Background()
	err := provider.fetchInitialPrices(ctx)
	require.NoError(t, err)

	provider.mu.RLock()
	defer provider.mu.RUnlock()

	assert.Equal(t, 50000.0, provider.prices["BTCUSDT"])
	assert.Equal(t, 2.5, provider.priceChanges["BTCUSDT"])
	assert.Equal(t, 500000000.0, provider.volumes24h["BTCUSDT"])

	assert.Equal(t, 3000.0, provider.prices["ETHUSDT"])
	assert.Equal(t, -1.5, provider.priceChanges["ETHUSDT"])

	_, ok := provider.prices["XRPUSDT"]
	assert.False(t, ok, "XRPUSDT should not be stored since it's not in pairs")
}

func TestStartDisabled(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Enabled = false
	provider := New(cfg, zap.NewNop())

	ctx := context.Background()
	err := provider.Start(ctx)
	assert.NoError(t, err)
}

func TestHandleTrade_InvalidJSON(t *testing.T) {
	cfg := DefaultConfig()
	provider := New(cfg, zap.NewNop())

	var emittedSignals []signal.Signal
	provider.Subscribe(func(ctx context.Context, sig signal.Signal) error {
		emittedSignals = append(emittedSignals, sig)
		return nil
	})

	ctx := context.Background()
	provider.handleTrade(ctx, json.RawMessage(`{invalid json}`))

	assert.Len(t, emittedSignals, 0)
}

func TestHandleTicker_InvalidJSON(t *testing.T) {
	cfg := DefaultConfig()
	provider := New(cfg, zap.NewNop())

	var emittedSignals []signal.Signal
	provider.Subscribe(func(ctx context.Context, sig signal.Signal) error {
		emittedSignals = append(emittedSignals, sig)
		return nil
	})

	ctx := context.Background()
	provider.handleTicker(ctx, json.RawMessage(`{invalid json}`))

	assert.Len(t, emittedSignals, 0)
}
