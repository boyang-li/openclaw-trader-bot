package tradingeconomics

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
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
	assert.NotEmpty(t, cfg.Countries)
	assert.NotEmpty(t, cfg.Indicators)
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

	assert.Equal(t, "trading_economics", p.Name())
	assert.Equal(t, signal.CategoryMacro, p.Category())
}

func TestNew_WithEmptyConfig(t *testing.T) {
	cfg := Config{
		APIKey: "test-key",
	}

	p, err := New(cfg, zap.NewNop())
	require.NoError(t, err)
	require.NotNil(t, p)

	// Should apply defaults
	assert.Equal(t, DefaultBaseURL, p.config.BaseURL)
	assert.Equal(t, DefaultPollInterval, p.config.PollInterval)
}

func TestNew_WithNilLogger(t *testing.T) {
	cfg := DefaultConfig()
	cfg.APIKey = "test-key"

	p, err := New(cfg, nil)
	require.NoError(t, err)
	require.NotNil(t, p)
}

func TestProvider_StartStop_Disabled(t *testing.T) {
	cfg := DefaultConfig()
	cfg.APIKey = "test-key"
	cfg.Enabled = false

	p, err := New(cfg, zap.NewNop())
	require.NoError(t, err)

	ctx := context.Background()
	err = p.Start(ctx)
	require.NoError(t, err)

	err = p.Stop()
	require.NoError(t, err)
}

func TestDetermineEventAction(t *testing.T) {
	tests := []struct {
		name     string
		event    CalendarEvent
		expected string
	}{
		{
			name:     "interest rate decision",
			event:    CalendarEvent{Category: "Interest Rate", Event: "Fed Interest Rate Decision"},
			expected: "rate_decision",
		},
		{
			name:     "GDP release",
			event:    CalendarEvent{Category: "GDP Growth Rate", Event: "GDP Growth Rate QoQ"},
			expected: "gdp_release",
		},
		{
			name:     "inflation release",
			event:    CalendarEvent{Category: "Inflation Rate", Event: "CPI YoY"},
			expected: "inflation_release",
		},
		{
			name:     "employment data",
			event:    CalendarEvent{Category: "Employment Change", Event: "Non-Farm Payrolls"},
			expected: "employment_release",
		},
		{
			name:     "PMI data",
			event:    CalendarEvent{Category: "Manufacturing PMI", Event: "Manufacturing PMI"},
			expected: "pmi_release",
		},
		{
			name:     "central bank speech",
			event:    CalendarEvent{Category: "Speech", Event: "Fed Chair Powell Speech"},
			expected: "central_bank_speech",
		},
		{
			name:     "trade balance",
			event:    CalendarEvent{Category: "Balance of Trade", Event: "Trade Balance"},
			expected: "trade_release",
		},
		{
			name:     "minutes release",
			event:    CalendarEvent{Category: "Other", Event: "FOMC Minutes"},
			expected: "minutes_release",
		},
		{
			name:     "generic release",
			event:    CalendarEvent{Category: "Consumer Confidence", Event: "Consumer Confidence"},
			expected: "economic_release",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := determineEventAction(tt.event)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestCalculateEventSentiment(t *testing.T) {
	tests := []struct {
		name         string
		event        CalendarEvent
		expectedSign float64
	}{
		{
			name: "beat expectations",
			event: CalendarEvent{
				Category: "GDP Growth Rate",
				Actual:   ptrFloat64(3.0),
				Forecast: ptrFloat64(2.5),
			},
			expectedSign: 1.0, // positive
		},
		{
			name: "miss expectations",
			event: CalendarEvent{
				Category: "GDP Growth Rate",
				Actual:   ptrFloat64(2.0),
				Forecast: ptrFloat64(2.5),
			},
			expectedSign: -1.0, // negative
		},
		{
			name: "unemployment beat (lower is better)",
			event: CalendarEvent{
				Category: "Unemployment Rate",
				Actual:   ptrFloat64(3.5),
				Forecast: ptrFloat64(4.0),
			},
			expectedSign: 1.0, // positive (lower is good for unemployment)
		},
		{
			name: "inflation higher than expected (bad)",
			event: CalendarEvent{
				Category: "Inflation Rate",
				Actual:   ptrFloat64(5.0),
				Forecast: ptrFloat64(4.0),
			},
			expectedSign: -1.0, // negative (higher inflation is bad, inverted)
		},
		{
			name: "no actual data",
			event: CalendarEvent{
				Category: "GDP Growth Rate",
				Actual:   nil,
				Forecast: ptrFloat64(2.5),
			},
			expectedSign: 0.0,
		},
		{
			name: "no forecast",
			event: CalendarEvent{
				Category: "GDP Growth Rate",
				Actual:   ptrFloat64(3.0),
				Forecast: nil,
			},
			expectedSign: 0.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := calculateEventSentiment(tt.event)
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

func TestCalculateEventUrgency(t *testing.T) {
	tests := []struct {
		name     string
		event    CalendarEvent
		expected signal.UrgencyLevel
	}{
		{
			name:     "high importance",
			event:    CalendarEvent{Importance: 3, Category: "Consumer Confidence"},
			expected: signal.UrgencyHigh,
		},
		{
			name:     "medium importance",
			event:    CalendarEvent{Importance: 2, Category: "Consumer Confidence"},
			expected: signal.UrgencyMedium,
		},
		{
			name:     "low importance",
			event:    CalendarEvent{Importance: 1, Category: "Consumer Confidence"},
			expected: signal.UrgencyLow,
		},
		{
			name:     "high impact indicator",
			event:    CalendarEvent{Importance: 1, Event: "Non Farm Payrolls"},
			expected: signal.UrgencyHigh,
		},
		{
			name:     "interest rate event",
			event:    CalendarEvent{Importance: 2, Category: "Interest Rate"},
			expected: signal.UrgencyHigh,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := calculateEventUrgency(tt.event)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestFormatEventResult(t *testing.T) {
	tests := []struct {
		name     string
		event    CalendarEvent
		contains []string
	}{
		{
			name: "pending event",
			event: CalendarEvent{
				Event:  "GDP Growth Rate",
				Actual: nil,
			},
			contains: []string{"GDP Growth Rate", "pending"},
		},
		{
			name: "beat",
			event: CalendarEvent{
				Event:    "GDP Growth Rate",
				Actual:   ptrFloat64(3.0),
				Forecast: ptrFloat64(2.5),
				Unit:     "%",
			},
			contains: []string{"GDP Growth Rate", "3.00", "%", "beat"},
		},
		{
			name: "miss",
			event: CalendarEvent{
				Event:    "GDP Growth Rate",
				Actual:   ptrFloat64(2.0),
				Forecast: ptrFloat64(2.5),
				Unit:     "%",
			},
			contains: []string{"GDP Growth Rate", "2.00", "%", "miss"},
		},
		{
			name: "inline",
			event: CalendarEvent{
				Event:    "GDP Growth Rate",
				Actual:   ptrFloat64(2.5),
				Forecast: ptrFloat64(2.5),
			},
			contains: []string{"GDP Growth Rate", "2.50", "inline"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := formatEventResult(tt.event)
			for _, substr := range tt.contains {
				assert.Contains(t, result, substr)
			}
		})
	}
}

func TestGenerateEventTags(t *testing.T) {
	tests := []struct {
		name       string
		event      CalendarEvent
		expectTags []string
	}{
		{
			name: "US GDP high impact beat",
			event: CalendarEvent{
				Country:    "United States",
				Category:   "GDP Growth Rate",
				Event:      "GDP Growth Rate QoQ",
				Importance: 3,
				Actual:     ptrFloat64(3.0),
				Forecast:   ptrFloat64(2.5),
			},
			expectTags: []string{"economic-calendar", "macro", "united-states", "gdp-growth-rate", "high-impact", "beat"},
		},
		{
			name: "EU interest rate medium impact miss",
			event: CalendarEvent{
				Country:    "Euro Area",
				Category:   "Interest Rate",
				Event:      "ECB Interest Rate Decision",
				Importance: 2,
				Actual:     ptrFloat64(4.0),
				Forecast:   ptrFloat64(4.25),
			},
			expectTags: []string{"economic-calendar", "macro", "euro-area", "central-bank", "rates", "medium-impact", "miss"},
		},
		{
			name: "Japan inflation",
			event: CalendarEvent{
				Country:    "Japan",
				Category:   "Inflation Rate",
				Event:      "CPI YoY",
				Importance: 2,
			},
			expectTags: []string{"economic-calendar", "macro", "japan", "inflation-rate", "medium-impact"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := generateEventTags(tt.event)
			for _, tag := range tt.expectTags {
				assert.Contains(t, result, tag)
			}
		})
	}
}

func TestAbsFloat(t *testing.T) {
	tests := []struct {
		input    float64
		expected float64
	}{
		{10.5, 10.5},
		{-10.5, 10.5},
		{0.0, 0.0},
		{-0.0, 0.0},
	}

	for _, tt := range tests {
		result := absFloat(tt.input)
		assert.Equal(t, tt.expected, result)
	}
}

func TestFetchCalendar_Success(t *testing.T) {
	mockResponse := []CalendarEvent{
		{
			ID:         "cal1",
			Date:       time.Now().Format("2006-01-02T15:04:05"),
			Country:    "United States",
			Category:   "GDP Growth Rate",
			Event:      "GDP Growth Rate QoQ",
			Actual:     ptrFloat64(3.0),
			Forecast:   ptrFloat64(2.5),
			Previous:   ptrFloat64(2.0),
			Importance: 3,
		},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "GET", r.Method)
		assert.Contains(t, r.URL.Path, "/calendar/country")
		assert.NotEmpty(t, r.URL.Query().Get("c"))

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(mockResponse)
	}))
	defer server.Close()

	cfg := Config{
		APIKey:    "test-api-key",
		BaseURL:   server.URL,
		Countries: []string{"united states"},
		Enabled:   true,
	}

	p, err := New(cfg, zap.NewNop())
	require.NoError(t, err)

	ctx := context.Background()
	events, err := p.fetchCalendar(ctx)

	require.NoError(t, err)
	assert.Len(t, events, 1)
	assert.Equal(t, "United States", events[0].Country)
}

func TestFetchCalendar_APIError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
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
	_, err = p.fetchCalendar(ctx)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "API error")
}

func TestEmitEventSignal(t *testing.T) {
	cfg := DefaultConfig()
	cfg.APIKey = "test-key"

	p, err := New(cfg, zap.NewNop())
	require.NoError(t, err)

	var capturedSignals []signal.Signal
	p.Subscribe(func(ctx context.Context, sig signal.Signal) error {
		capturedSignals = append(capturedSignals, sig)
		return nil
	})

	event := CalendarEvent{
		ID:         "test-event",
		Date:       time.Now().Format("2006-01-02T15:04:05"),
		Country:    "United States",
		Category:   "Interest Rate",
		Event:      "Fed Interest Rate Decision",
		Actual:     ptrFloat64(5.25),
		Forecast:   ptrFloat64(5.00),
		Previous:   ptrFloat64(5.00),
		Importance: 3,
		Unit:       "%",
	}

	ctx := context.Background()
	err = p.emitEventSignal(ctx, event)
	require.NoError(t, err)

	require.Len(t, capturedSignals, 1)
	sig := capturedSignals[0]

	assert.Equal(t, signal.SourceTradingEcon, sig.Source)
	assert.Equal(t, signal.CategoryMacro, sig.Category)
	assert.Equal(t, "United States", sig.Subject)
	assert.Equal(t, "rate_decision", sig.Action)
	assert.Contains(t, sig.Object, "Fed Interest Rate Decision")
	assert.Contains(t, sig.Object, "beat")
	assert.Equal(t, signal.UrgencyHigh, sig.Urgency)
	assert.Contains(t, sig.Tags, "central-bank")
	assert.Contains(t, sig.Tags, "rates")
}

func TestCleanupSeenEvents(t *testing.T) {
	cfg := DefaultConfig()
	cfg.APIKey = "test-key"

	p, err := New(cfg, zap.NewNop())
	require.NoError(t, err)

	// Add some events
	p.seenEvents["old_event"] = time.Now().Add(-48 * time.Hour) // Old
	p.seenEvents["new_event"] = time.Now()                      // Recent

	p.cleanupSeenEvents()

	assert.NotContains(t, p.seenEvents, "old_event")
	assert.Contains(t, p.seenEvents, "new_event")
}

// Helper function
func ptrFloat64(f float64) *float64 {
	return &f
}
