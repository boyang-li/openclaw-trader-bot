package cot

import (
	"context"
	"strings"
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
	assert.True(t, cfg.Enabled)
	assert.NotEmpty(t, cfg.Contracts)
}

func TestNew_WithDefaults(t *testing.T) {
	cfg := DefaultConfig()

	p := New(cfg, zap.NewNop())
	require.NotNil(t, p)

	assert.Equal(t, "cme_cot", p.Name())
	assert.Equal(t, signal.CategoryMacro, p.Category())
}

func TestNew_WithEmptyConfig(t *testing.T) {
	cfg := Config{}

	p := New(cfg, zap.NewNop())
	require.NotNil(t, p)

	// Should apply defaults
	assert.Equal(t, DefaultBaseURL, p.config.BaseURL)
	assert.Equal(t, DefaultPollInterval, p.config.PollInterval)
}

func TestNew_WithNilLogger(t *testing.T) {
	cfg := DefaultConfig()

	p := New(cfg, nil)
	require.NotNil(t, p)
}

func TestProvider_StartStop_Disabled(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Enabled = false

	p := New(cfg, zap.NewNop())

	ctx := context.Background()
	err := p.Start(ctx)
	require.NoError(t, err)

	err = p.Stop()
	require.NoError(t, err)
}

func TestDetermineCOTAction(t *testing.T) {
	tests := []struct {
		name     string
		cot      *COTData
		prev     *COTData
		expected string
	}{
		{
			name: "no previous data",
			cot: &COTData{
				NetNonComm: 10000,
			},
			prev:     nil,
			expected: "cot_report",
		},
		{
			name: "position increase",
			cot: &COTData{
				NetNonComm: 15000,
			},
			prev: &COTData{
				NetNonComm: 10000,
			},
			expected: "spec_position_increase",
		},
		{
			name: "position decrease",
			cot: &COTData{
				NetNonComm: 5000,
			},
			prev: &COTData{
				NetNonComm: 10000,
			},
			expected: "spec_position_decrease",
		},
		{
			name: "no change",
			cot: &COTData{
				NetNonComm: 10000,
			},
			prev: &COTData{
				NetNonComm: 10000,
			},
			expected: "cot_report",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := determineCOTAction(tt.cot, tt.prev)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestCalculateCOTSentiment(t *testing.T) {
	tests := []struct {
		name         string
		cot          *COTData
		prev         *COTData
		expectedSign float64 // positive, negative, or zero
	}{
		{
			name: "strong net long",
			cot: &COTData{
				NetNonComm:   50000,
				OpenInterest: 100000,
			},
			prev:         nil,
			expectedSign: 1.0,
		},
		{
			name: "strong net short",
			cot: &COTData{
				NetNonComm:   -50000,
				OpenInterest: 100000,
			},
			prev:         nil,
			expectedSign: -1.0,
		},
		{
			name: "neutral",
			cot: &COTData{
				NetNonComm:   0,
				OpenInterest: 100000,
			},
			prev:         nil,
			expectedSign: 0.0,
		},
		{
			name: "zero open interest",
			cot: &COTData{
				NetNonComm:   10000,
				OpenInterest: 0,
			},
			prev:         nil,
			expectedSign: 0.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := calculateCOTSentiment(tt.cot, tt.prev)
			if tt.expectedSign > 0 {
				assert.Greater(t, result, 0.0)
			} else if tt.expectedSign < 0 {
				assert.Less(t, result, 0.0)
			} else {
				assert.Equal(t, 0.0, result)
			}
			// Ensure within bounds
			assert.GreaterOrEqual(t, result, -1.0)
			assert.LessOrEqual(t, result, 1.0)
		})
	}
}

func TestCalculateCOTUrgency(t *testing.T) {
	tests := []struct {
		name     string
		cot      *COTData
		prev     *COTData
		expected signal.UrgencyLevel
	}{
		{
			name: "no previous data",
			cot: &COTData{
				NetNonComm:   50000,
				OpenInterest: 100000,
			},
			prev:     nil,
			expected: signal.UrgencyLow,
		},
		{
			name: "large change",
			cot: &COTData{
				NetNonComm:   20000,
				OpenInterest: 100000,
			},
			prev: &COTData{
				NetNonComm:   5000,
				OpenInterest: 100000,
			},
			expected: signal.UrgencyHigh, // 15% change
		},
		{
			name: "medium change",
			cot: &COTData{
				NetNonComm:   12000,
				OpenInterest: 100000,
			},
			prev: &COTData{
				NetNonComm:   5000,
				OpenInterest: 100000,
			},
			expected: signal.UrgencyMedium, // 7% change
		},
		{
			name: "small change",
			cot: &COTData{
				NetNonComm:   6000,
				OpenInterest: 100000,
			},
			prev: &COTData{
				NetNonComm:   5000,
				OpenInterest: 100000,
			},
			expected: signal.UrgencyLow, // 1% change
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := calculateCOTUrgency(tt.cot, tt.prev)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSimplifyContractName(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"GOLD - COMMODITY EXCHANGE INC.", "Gold"},
		{"SILVER - COMMODITY EXCHANGE INC.", "Silver"},
		{"WTI CRUDE OIL - NEW YORK MERCANTILE EXCHANGE", "WTI Crude"},
		{"E-MINI S&P 500 - CHICAGO MERCANTILE EXCHANGE", "S&P 500"},
		{"BITCOIN - CHICAGO MERCANTILE EXCHANGE", "Bitcoin CME"},
		{"EURO FX - CHICAGO MERCANTILE EXCHANGE", "EUR/USD"},
		{"JAPANESE YEN - CHICAGO MERCANTILE EXCHANGE", "USD/JPY"},
		{"UNKNOWN CONTRACT - SOME EXCHANGE", "Unknown Contract"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := simplifyContractName(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestFormatPositioning(t *testing.T) {
	tests := []struct {
		name     string
		cot      *COTData
		expected string
	}{
		{
			name:     "net long",
			cot:      &COTData{NetNonComm: 50000},
			expected: "Speculators net long 50000 contracts",
		},
		{
			name:     "net short",
			cot:      &COTData{NetNonComm: -30000},
			expected: "Speculators net short 30000 contracts",
		},
		{
			name:     "neutral",
			cot:      &COTData{NetNonComm: 0},
			expected: "Speculators neutral 0 contracts",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := formatPositioning(tt.cot)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGenerateCOTTags(t *testing.T) {
	tests := []struct {
		name       string
		cot        *COTData
		prev       *COTData
		expectTags []string
		rejectTags []string
	}{
		{
			name: "gold long increasing",
			cot: &COTData{
				ContractName: "GOLD - COMMODITY EXCHANGE INC.",
				NetNonComm:   50000,
			},
			prev: &COTData{
				NetNonComm: 40000,
			},
			expectTags: []string{"cot", "futures", "positioning", "gold", "metals", "spec-long", "increasing-longs"},
		},
		{
			name: "sp500 short increasing",
			cot: &COTData{
				ContractName: "E-MINI S&P 500 - CHICAGO MERCANTILE EXCHANGE",
				NetNonComm:   -10000,
			},
			prev: &COTData{
				NetNonComm: -5000,
			},
			expectTags: []string{"cot", "futures", "positioning", "equity-index", "spec-short", "increasing-shorts"},
		},
		{
			name: "bitcoin no previous",
			cot: &COTData{
				ContractName: "BITCOIN - CHICAGO MERCANTILE EXCHANGE",
				NetNonComm:   1000,
			},
			prev:       nil,
			expectTags: []string{"cot", "futures", "positioning", "crypto", "spec-long"},
			rejectTags: []string{"increasing-longs", "increasing-shorts"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := generateCOTTags(tt.cot, tt.prev)

			for _, tag := range tt.expectTags {
				assert.Contains(t, result, tag)
			}
			for _, tag := range tt.rejectTags {
				assert.NotContains(t, result, tag)
			}
		})
	}
}

func TestParseDate(t *testing.T) {
	tests := []struct {
		input    string
		expected string
		hasError bool
	}{
		{"2024-01-15", "2024-01-15", false},
		{"240115", "2024-01-15", false},
		{"01/15/2024", "2024-01-15", false},
		{"1/5/2024", "2024-01-05", false},
		{"invalid", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result, err := parseDate(tt.input)
			if tt.hasError {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, result.Format("2006-01-02"))
			}
		})
	}
}

func TestAbs(t *testing.T) {
	tests := []struct {
		input    int64
		expected int64
	}{
		{10, 10},
		{-10, 10},
		{0, 0},
		{-9223372036854775807, 9223372036854775807},
	}

	for _, tt := range tests {
		result := abs(tt.input)
		assert.Equal(t, tt.expected, result)
	}
}

func TestContractFilters(t *testing.T) {
	cfg := Config{
		Contracts: []string{"GOLD - COMMODITY EXCHANGE INC.", "silver - commodity exchange inc."},
		Enabled:   true,
	}

	p := New(cfg, zap.NewNop())

	// Check that filters are case-insensitive
	assert.True(t, p.contractFilters["GOLD - COMMODITY EXCHANGE INC."])
	assert.True(t, p.contractFilters["SILVER - COMMODITY EXCHANGE INC."])
}

func TestEmitCOTSignal(t *testing.T) {
	cfg := DefaultConfig()
	p := New(cfg, zap.NewNop())

	var capturedSignals []signal.Signal
	p.Subscribe(func(ctx context.Context, sig signal.Signal) error {
		capturedSignals = append(capturedSignals, sig)
		return nil
	})

	cot := &COTData{
		ContractName: "GOLD - COMMODITY EXCHANGE INC.",
		ReportDate:   time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
		OpenInterest: 500000,
		NonCommLong:  150000,
		NonCommShort: 100000,
		CommLong:     200000,
		CommShort:    250000,
		NetNonComm:   50000,
		NetComm:      -50000,
	}

	ctx := context.Background()
	err := p.emitCOTSignal(ctx, cot, nil)
	require.NoError(t, err)

	require.Len(t, capturedSignals, 1)
	sig := capturedSignals[0]

	assert.Equal(t, signal.SourceCMECOT, sig.Source)
	assert.Equal(t, signal.CategoryMacro, sig.Category)
	assert.Equal(t, "Gold", sig.Subject)
	assert.Equal(t, "cot_report", sig.Action)
	assert.Contains(t, strings.ToLower(sig.Object), "net long")
	assert.Greater(t, sig.Sentiment, 0.0) // Net long should be bullish
	assert.Contains(t, sig.Tags, "gold")
	assert.Contains(t, sig.Tags, "metals")
	assert.Contains(t, sig.Tags, "spec-long")
}
