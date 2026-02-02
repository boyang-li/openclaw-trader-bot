package whalealert

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

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	assert.Equal(t, DefaultBaseURL, cfg.BaseURL)
	assert.Equal(t, DefaultPollInterval, cfg.PollInterval)
	assert.Equal(t, int64(DefaultMinValueUSD), cfg.MinValueUSD)
	assert.True(t, cfg.Enabled)
}

func TestNew_RequiresAPIKey(t *testing.T) {
	cfg := DefaultConfig()
	cfg.APIKey = ""

	_, err := New(cfg, zap.NewNop())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "API key is required")
}

func TestNew_Success(t *testing.T) {
	cfg := DefaultConfig()
	cfg.APIKey = "test-api-key"

	p, err := New(cfg, zap.NewNop())
	require.NoError(t, err)
	require.NotNil(t, p)

	assert.Equal(t, "whale_alert", p.Name())
	assert.Equal(t, signal.CategoryCrypto, p.Category())
}

func TestProvider_StartStop(t *testing.T) {
	cfg := DefaultConfig()
	cfg.APIKey = "test-api-key"
	cfg.Enabled = false

	p, err := New(cfg, zap.NewNop())
	require.NoError(t, err)

	ctx := context.Background()
	err = p.Start(ctx)
	require.NoError(t, err)

	err = p.Stop()
	require.NoError(t, err)
}

func TestMapTransactionAction(t *testing.T) {
	tests := []struct {
		txType   string
		expected string
	}{
		{"transfer", "whale_transfer"},
		{"mint", "whale_mint"},
		{"burn", "whale_burn"},
		{"lock", "whale_lock"},
		{"unlock", "whale_unlock"},
		{"TRANSFER", "whale_transfer"},
		{"unknown", "whale_unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.txType, func(t *testing.T) {
			result := mapTransactionAction(tt.txType)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestCalculateSentiment(t *testing.T) {
	tests := []struct {
		name     string
		tx       Transaction
		expected float64
	}{
		{
			name: "burn is bullish",
			tx: Transaction{
				TransactionType: "burn",
			},
			expected: 0.5,
		},
		{
			name: "mint is bearish",
			tx: Transaction{
				TransactionType: "mint",
			},
			expected: -0.3,
		},
		{
			name: "exchange deposit is bearish",
			tx: Transaction{
				TransactionType: "transfer",
				From:            Owner{OwnerType: "unknown"},
				To:              Owner{OwnerType: "exchange"},
			},
			expected: -0.4,
		},
		{
			name: "exchange withdrawal is bullish",
			tx: Transaction{
				TransactionType: "transfer",
				From:            Owner{OwnerType: "exchange"},
				To:              Owner{OwnerType: "unknown"},
			},
			expected: 0.4,
		},
		{
			name: "exchange to exchange is neutral",
			tx: Transaction{
				TransactionType: "transfer",
				From:            Owner{OwnerType: "exchange"},
				To:              Owner{OwnerType: "exchange"},
			},
			expected: 0.0,
		},
		{
			name: "lock is bullish",
			tx: Transaction{
				TransactionType: "lock",
			},
			expected: 0.3,
		},
		{
			name: "unlock is slightly bearish",
			tx: Transaction{
				TransactionType: "unlock",
			},
			expected: -0.2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := calculateSentiment(tt.tx)
			assert.InDelta(t, tt.expected, result, 0.01)
		})
	}
}

func TestCalculateUrgency(t *testing.T) {
	tests := []struct {
		amountUSD float64
		expected  signal.UrgencyLevel
	}{
		{500_000, signal.UrgencyLow},
		{5_000_000, signal.UrgencyLow},
		{15_000_000, signal.UrgencyMedium},
		{75_000_000, signal.UrgencyHigh},
		{150_000_000, signal.UrgencyCritical},
	}

	for _, tt := range tests {
		t.Run(tt.expected.String(), func(t *testing.T) {
			result := calculateUrgency(tt.amountUSD)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestShortenAddress(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"0x123", "0x123"},
		{"short", "short"},
		{"0x1234567890abcdef1234567890abcdef12345678", "0x1234...5678"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := shortenAddress(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestBuildSubject(t *testing.T) {
	tests := []struct {
		name     string
		tx       Transaction
		expected string
	}{
		{
			name: "with owner name",
			tx: Transaction{
				From: Owner{Owner: "Binance", OwnerType: "exchange", Address: "0x123456789abcdef"},
			},
			expected: "Binance",
		},
		{
			name: "with owner type only",
			tx: Transaction{
				From: Owner{OwnerType: "exchange", Address: "0x1234567890abcdef1234567890abcdef12345678"},
			},
			expected: "exchange (0x1234...5678)",
		},
		{
			name: "with address only",
			tx: Transaction{
				From: Owner{Address: "0x1234567890abcdef1234567890abcdef12345678"},
			},
			expected: "0x1234...5678",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := buildSubject(tt.tx)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGenerateTags(t *testing.T) {
	tx := Transaction{
		Blockchain:      "ethereum",
		Symbol:          "ETH",
		TransactionType: "transfer",
		AmountUSD:       15_000_000,
		From:            Owner{Owner: "Binance", OwnerType: "exchange"},
		To:              Owner{OwnerType: "unknown"},
	}

	tags := generateTags(tx)

	assert.Contains(t, tags, "whale")
	assert.Contains(t, tags, "ethereum")
	assert.Contains(t, tags, "eth")
	assert.Contains(t, tags, "transfer")
	assert.Contains(t, tags, ">$10M")
	assert.Contains(t, tags, "exchange-outflow")
	assert.Contains(t, tags, "from:binance")
}

func TestPassesFilters(t *testing.T) {
	cfg := DefaultConfig()
	cfg.APIKey = "test"
	p, _ := New(cfg, zap.NewNop())

	tx := Transaction{
		Blockchain:      "bitcoin",
		TransactionType: "transfer",
	}
	assert.True(t, p.passesFilters(tx))

	p.config.Blockchains = []string{"ethereum"}
	assert.False(t, p.passesFilters(tx))

	p.config.Blockchains = []string{"bitcoin", "ethereum"}
	assert.True(t, p.passesFilters(tx))

	p.config.Blockchains = []string{}
	p.config.TransactionTypes = []string{"mint", "burn"}
	assert.False(t, p.passesFilters(tx))

	p.config.TransactionTypes = []string{"transfer"}
	assert.True(t, p.passesFilters(tx))
}

func TestFetchTransactions_Success(t *testing.T) {
	mockResponse := apiResponse{
		Result: "success",
		Count:  2,
		Cursor: "next-cursor",
		Transactions: []Transaction{
			{
				ID:              "tx1",
				Blockchain:      "ethereum",
				Symbol:          "ETH",
				TransactionType: "transfer",
				Hash:            "0xabc123",
				Timestamp:       time.Now().Unix(),
				Amount:          1000,
				AmountUSD:       2_500_000,
				From:            Owner{Address: "0x111", Owner: "Binance", OwnerType: "exchange"},
				To:              Owner{Address: "0x222", OwnerType: "unknown"},
			},
			{
				ID:              "tx2",
				Blockchain:      "bitcoin",
				Symbol:          "BTC",
				TransactionType: "transfer",
				Hash:            "btc456",
				Timestamp:       time.Now().Unix(),
				Amount:          50,
				AmountUSD:       3_000_000,
				From:            Owner{Address: "bc1xxx"},
				To:              Owner{Address: "bc1yyy", Owner: "Coinbase", OwnerType: "exchange"},
			},
		},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "GET", r.Method)
		assert.Contains(t, r.URL.Path, "/transactions")
		assert.Equal(t, "test-api-key", r.URL.Query().Get("api_key"))
		assert.NotEmpty(t, r.URL.Query().Get("min_value"))
		assert.NotEmpty(t, r.URL.Query().Get("start"))

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(mockResponse)
	}))
	defer server.Close()

	cfg := Config{
		APIKey:      "test-api-key",
		BaseURL:     server.URL,
		MinValueUSD: 1_000_000,
		Enabled:     true,
	}

	p, err := New(cfg, zap.NewNop())
	require.NoError(t, err)

	ctx := context.Background()
	txs, cursor, err := p.fetchTransactions(ctx, time.Now().Add(-5*time.Minute), "")

	require.NoError(t, err)
	assert.Equal(t, "next-cursor", cursor)
	assert.Len(t, txs, 2)
	assert.Equal(t, "tx1", txs[0].ID)
	assert.Equal(t, "ethereum", txs[0].Blockchain)
}

func TestFetchTransactions_APIError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		json.NewEncoder(w).Encode(apiResponse{
			Result:  "error",
			Message: "Invalid API key",
		})
	}))
	defer server.Close()

	cfg := Config{
		APIKey:  "invalid-key",
		BaseURL: server.URL,
		Enabled: true,
	}

	p, err := New(cfg, zap.NewNop())
	require.NoError(t, err)

	ctx := context.Background()
	_, _, err = p.fetchTransactions(ctx, time.Now().Add(-5*time.Minute), "")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "API error")
}

func TestEmitTransactionSignal(t *testing.T) {
	cfg := DefaultConfig()
	cfg.APIKey = "test"

	p, err := New(cfg, zap.NewNop())
	require.NoError(t, err)

	var capturedSignals []signal.Signal
	var mu sync.Mutex

	p.Subscribe(func(ctx context.Context, sig signal.Signal) error {
		mu.Lock()
		capturedSignals = append(capturedSignals, sig)
		mu.Unlock()
		return nil
	})

	tx := Transaction{
		ID:              "tx-test",
		Blockchain:      "ethereum",
		Symbol:          "ETH",
		TransactionType: "transfer",
		Hash:            "0xhash",
		Timestamp:       time.Now().Unix(),
		Amount:          500,
		AmountUSD:       1_250_000,
		From:            Owner{Address: "0xfrom", Owner: "Binance", OwnerType: "exchange"},
		To:              Owner{Address: "0xto", OwnerType: "unknown"},
	}

	ctx := context.Background()
	err = p.emitTransactionSignal(ctx, tx)
	require.NoError(t, err)

	require.Len(t, capturedSignals, 1)
	sig := capturedSignals[0]

	assert.Equal(t, signal.SourceWhaleAlert, sig.Source)
	assert.Equal(t, signal.CategoryCrypto, sig.Category)
	assert.Equal(t, "Binance", sig.Subject)
	assert.Equal(t, "whale_transfer", sig.Action)
	assert.InDelta(t, 0.4, sig.Sentiment, 0.01)
	assert.Equal(t, signal.UrgencyLow, sig.Urgency)
	assert.Contains(t, sig.Tags, "exchange-outflow")
}
