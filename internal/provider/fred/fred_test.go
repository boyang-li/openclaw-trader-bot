package fred

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
	cfg.APIKey = "test-api-key"
	provider, err := New(cfg, nil)

	require.NoError(t, err)
	assert.NotNil(t, provider)
	assert.Equal(t, "fred", provider.Name())
	assert.Equal(t, signal.CategoryMacro, provider.Category())
}

func TestNew_WithLogger(t *testing.T) {
	cfg := DefaultConfig()
	cfg.APIKey = "test-api-key"
	logger := zap.NewNop()
	provider, err := New(cfg, logger)

	require.NoError(t, err)
	assert.NotNil(t, provider)
}

func TestNew_MissingAPIKey(t *testing.T) {
	cfg := DefaultConfig()
	cfg.APIKey = ""

	provider, err := New(cfg, nil)

	assert.Nil(t, provider)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "API key is required")
}

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	assert.Equal(t, DefaultBaseURL, cfg.BaseURL)
	assert.Equal(t, DefaultPollInterval, cfg.PollInterval)
	assert.True(t, cfg.Enabled)
	assert.NotEmpty(t, cfg.Series)
}

func TestDefaultSeries(t *testing.T) {
	series := DefaultSeries()

	assert.NotEmpty(t, series)

	expectedIDs := []string{"DFF", "T10Y2Y", "UNRATE", "CPIAUCSL", "GDP", "MORTGAGE30US", "DTWEXBGS", "VIXCLS"}
	actualIDs := make([]string, len(series))
	for i, s := range series {
		actualIDs[i] = s.ID
	}
	assert.Equal(t, expectedIDs, actualIDs)

	for _, s := range series {
		assert.NotEmpty(t, s.ID)
		assert.NotEmpty(t, s.Title)
		assert.NotEmpty(t, s.Tags)
		assert.NotNil(t, s.Sentiment)
	}
}

func TestDefaultSeries_SentimentFunctions(t *testing.T) {
	series := DefaultSeries()

	t.Run("DFF rising rates negative", func(t *testing.T) {
		dff := findSeries(series, "DFF")
		require.NotNil(t, dff)

		sentiment := dff.Sentiment(5.5, 5.0)
		assert.Less(t, sentiment, 0.0)

		sentiment = dff.Sentiment(5.0, 5.5)
		assert.Greater(t, sentiment, 0.0)
	})

	t.Run("T10Y2Y inverted yield curve negative", func(t *testing.T) {
		t10y2y := findSeries(series, "T10Y2Y")
		require.NotNil(t, t10y2y)

		sentiment := t10y2y.Sentiment(-0.5, 0.0)
		assert.Less(t, sentiment, 0.0)

		sentiment = t10y2y.Sentiment(0.5, 0.0)
		assert.Greater(t, sentiment, 0.0)
	})

	t.Run("UNRATE rising unemployment negative", func(t *testing.T) {
		unrate := findSeries(series, "UNRATE")
		require.NotNil(t, unrate)

		sentiment := unrate.Sentiment(4.0, 3.5)
		assert.Less(t, sentiment, 0.0)

		sentiment = unrate.Sentiment(3.5, 4.0)
		assert.Greater(t, sentiment, 0.0)
	})

	t.Run("CPIAUCSL high inflation negative", func(t *testing.T) {
		cpi := findSeries(series, "CPIAUCSL")
		require.NotNil(t, cpi)

		sentiment := cpi.Sentiment(310.0, 300.0)
		assert.Less(t, sentiment, 0.0)

		sentiment = cpi.Sentiment(300.1, 300.0)
		assert.Greater(t, sentiment, 0.0)
	})

	t.Run("GDP growth positive", func(t *testing.T) {
		gdp := findSeries(series, "GDP")
		require.NotNil(t, gdp)

		sentiment := gdp.Sentiment(25000.0, 24000.0)
		assert.Greater(t, sentiment, 0.0)

		sentiment = gdp.Sentiment(24000.0, 25000.0)
		assert.Less(t, sentiment, 0.0)
	})

	t.Run("MORTGAGE30US rising rates negative", func(t *testing.T) {
		mortgage := findSeries(series, "MORTGAGE30US")
		require.NotNil(t, mortgage)

		sentiment := mortgage.Sentiment(7.0, 6.5)
		assert.Less(t, sentiment, 0.0)

		sentiment = mortgage.Sentiment(6.5, 7.0)
		assert.Greater(t, sentiment, 0.0)
	})

	t.Run("DTWEXBGS dollar strength", func(t *testing.T) {
		dxy := findSeries(series, "DTWEXBGS")
		require.NotNil(t, dxy)

		sentiment := dxy.Sentiment(110.0, 100.0)
		assert.Greater(t, sentiment, 0.0)

		sentiment = dxy.Sentiment(90.0, 100.0)
		assert.Less(t, sentiment, 0.0)

		sentiment = dxy.Sentiment(100.5, 100.0)
		assert.Equal(t, 0.0, sentiment)
	})

	t.Run("VIXCLS fear levels", func(t *testing.T) {
		vix := findSeries(series, "VIXCLS")
		require.NotNil(t, vix)

		sentiment := vix.Sentiment(35.0, 30.0)
		assert.Less(t, sentiment, -0.5)

		sentiment = vix.Sentiment(25.0, 20.0)
		assert.Less(t, sentiment, 0.0)
		assert.Greater(t, sentiment, -0.5)

		sentiment = vix.Sentiment(15.0, 10.0)
		assert.Greater(t, sentiment, 0.0)
	})
}

func findSeries(series []SeriesMetadata, id string) *SeriesMetadata {
	for i := range series {
		if series[i].ID == id {
			return &series[i]
		}
	}
	return nil
}

func TestFetchSeries(t *testing.T) {
	tests := []struct {
		name           string
		responseBody   fredResponse
		statusCode     int
		expectSignal   bool
		expectError    bool
		expectedAction string
	}{
		{
			name: "significant increase over 5pct",
			responseBody: fredResponse{
				Observations: []fredObservation{
					{Date: "2024-01-15", Value: "5.50"},
					{Date: "2024-01-14", Value: "5.00"},
				},
			},
			statusCode:     http.StatusOK,
			expectSignal:   true,
			expectError:    false,
			expectedAction: "significant_increase",
		},
		{
			name: "significant decrease",
			responseBody: fredResponse{
				Observations: []fredObservation{
					{Date: "2024-01-15", Value: "4.75"},
					{Date: "2024-01-14", Value: "5.25"},
				},
			},
			statusCode:     http.StatusOK,
			expectSignal:   true,
			expectError:    false,
			expectedAction: "significant_decrease",
		},
		{
			name: "normal data release",
			responseBody: fredResponse{
				Observations: []fredObservation{
					{Date: "2024-01-15", Value: "5.30"},
					{Date: "2024-01-14", Value: "5.25"},
				},
			},
			statusCode:     http.StatusOK,
			expectSignal:   true,
			expectError:    false,
			expectedAction: "data_release",
		},
		{
			name: "no data available",
			responseBody: fredResponse{
				Observations: []fredObservation{
					{Date: "2024-01-15", Value: "."},
				},
			},
			statusCode:   http.StatusOK,
			expectSignal: false,
			expectError:  false,
		},
		{
			name: "empty observations",
			responseBody: fredResponse{
				Observations: []fredObservation{},
			},
			statusCode:   http.StatusOK,
			expectSignal: false,
			expectError:  false,
		},
		{
			name:         "server error",
			responseBody: fredResponse{},
			statusCode:   http.StatusInternalServerError,
			expectSignal: false,
			expectError:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, "/series/observations", r.URL.Path)
				assert.Equal(t, "DFF", r.URL.Query().Get("series_id"))
				assert.Equal(t, "test-key", r.URL.Query().Get("api_key"))
				assert.Equal(t, "json", r.URL.Query().Get("file_type"))
				assert.Equal(t, "desc", r.URL.Query().Get("sort_order"))
				assert.Equal(t, "2", r.URL.Query().Get("limit"))

				w.WriteHeader(tt.statusCode)
				if tt.statusCode == http.StatusOK {
					json.NewEncoder(w).Encode(tt.responseBody)
				}
			}))
			defer server.Close()

			cfg := DefaultConfig()
			cfg.APIKey = "test-key"
			cfg.BaseURL = server.URL
			provider, err := New(cfg, zap.NewNop())
			require.NoError(t, err)

			series := SeriesMetadata{
				ID:    "DFF",
				Title: "Federal Funds Rate",
				Tags:  []string{"interest-rate", "fed"},
				Sentiment: func(val, prev float64) float64 {
					if val > prev {
						return -0.3
					}
					return 0.2
				},
				Urgency: signal.UrgencyMedium,
			}

			ctx := context.Background()
			sig, err := provider.fetchSeries(ctx, series)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			if tt.expectSignal {
				require.NotNil(t, sig)
				assert.Equal(t, signal.SourceFRED, sig.Source)
				assert.Equal(t, signal.CategoryMacro, sig.Category)
				assert.Equal(t, "DFF", sig.Subject)
				assert.Equal(t, tt.expectedAction, sig.Action)
				assert.Equal(t, "US Economy", sig.Object)
				assert.Equal(t, 0.95, sig.Confidence)
			} else if !tt.expectError {
				assert.Nil(t, sig)
			}
		})
	}
}

func TestFetchSeries_UsesStoredPreviousValue(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		response := fredResponse{
			Observations: []fredObservation{
				{Date: "2024-01-15", Value: "5.50"},
			},
		}
		json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	cfg := DefaultConfig()
	cfg.APIKey = "test-key"
	cfg.BaseURL = server.URL
	provider, err := New(cfg, zap.NewNop())
	require.NoError(t, err)

	provider.mu.Lock()
	provider.lastValues["DFF"] = 5.00
	provider.mu.Unlock()

	series := SeriesMetadata{
		ID:    "DFF",
		Title: "Federal Funds Rate",
		Tags:  []string{"interest-rate"},
		Sentiment: func(val, prev float64) float64 {
			if val > prev {
				return -0.3
			}
			return 0.2
		},
		Urgency: signal.UrgencyMedium,
	}

	ctx := context.Background()
	sig, err := provider.fetchSeries(ctx, series)

	require.NoError(t, err)
	require.NotNil(t, sig)
	assert.Equal(t, "significant_increase", sig.Action)
	assert.Equal(t, -0.3, sig.Sentiment)
}

func TestPoll(t *testing.T) {
	callCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		response := fredResponse{
			Observations: []fredObservation{
				{Date: "2024-01-15", Value: "5.33"},
				{Date: "2024-01-14", Value: "5.25"},
			},
		}
		json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	cfg := DefaultConfig()
	cfg.APIKey = "test-key"
	cfg.BaseURL = server.URL
	cfg.Series = []SeriesMetadata{
		{
			ID:        "DFF",
			Title:     "Federal Funds Rate",
			Tags:      []string{"rate"},
			Sentiment: func(_, _ float64) float64 { return 0 },
			Urgency:   signal.UrgencyMedium,
		},
		{
			ID:        "UNRATE",
			Title:     "Unemployment Rate",
			Tags:      []string{"employment"},
			Sentiment: func(_, _ float64) float64 { return 0 },
			Urgency:   signal.UrgencyMedium,
		},
	}

	provider, err := New(cfg, zap.NewNop())
	require.NoError(t, err)

	var emittedSignals []signal.Signal
	var mu sync.Mutex
	provider.Subscribe(func(ctx context.Context, sig signal.Signal) error {
		mu.Lock()
		emittedSignals = append(emittedSignals, sig)
		mu.Unlock()
		return nil
	})

	ctx := context.Background()
	provider.poll(ctx)

	assert.Equal(t, 2, callCount)

	mu.Lock()
	assert.Len(t, emittedSignals, 2)
	mu.Unlock()
}

func TestStartDisabled(t *testing.T) {
	cfg := DefaultConfig()
	cfg.APIKey = "test-key"
	cfg.Enabled = false

	provider, err := New(cfg, zap.NewNop())
	require.NoError(t, err)

	ctx := context.Background()
	err = provider.Start(ctx)
	assert.NoError(t, err)
}

func TestFetchSeries_InvalidValueFormat(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		response := fredResponse{
			Observations: []fredObservation{
				{Date: "2024-01-15", Value: "not-a-number"},
			},
		}
		json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	cfg := DefaultConfig()
	cfg.APIKey = "test-key"
	cfg.BaseURL = server.URL
	provider, err := New(cfg, zap.NewNop())
	require.NoError(t, err)

	series := SeriesMetadata{
		ID:        "DFF",
		Title:     "Federal Funds Rate",
		Tags:      []string{"rate"},
		Sentiment: func(_, _ float64) float64 { return 0 },
		Urgency:   signal.UrgencyMedium,
	}

	ctx := context.Background()
	sig, err := provider.fetchSeries(ctx, series)

	assert.Error(t, err)
	assert.Nil(t, sig)
	assert.Contains(t, err.Error(), "parse value")
}

func TestFetchSeries_SignalRawData(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		response := fredResponse{
			Observations: []fredObservation{
				{Date: "2024-01-15", Value: "5.50"},
				{Date: "2024-01-14", Value: "5.25"},
			},
		}
		json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	cfg := DefaultConfig()
	cfg.APIKey = "test-key"
	cfg.BaseURL = server.URL
	provider, err := New(cfg, zap.NewNop())
	require.NoError(t, err)

	series := SeriesMetadata{
		ID:        "T10Y2Y",
		Title:     "10-Year Treasury Minus 2-Year",
		Tags:      []string{"yield-curve"},
		Sentiment: func(_, _ float64) float64 { return 0.1 },
		Urgency:   signal.UrgencyHigh,
	}

	ctx := context.Background()
	sig, err := provider.fetchSeries(ctx, series)

	require.NoError(t, err)
	require.NotNil(t, sig)

	var rawData map[string]interface{}
	err = json.Unmarshal(sig.RawData, &rawData)
	require.NoError(t, err)

	assert.Equal(t, "T10Y2Y", rawData["series_id"])
	assert.Equal(t, "10-Year Treasury Minus 2-Year", rawData["series_title"])
	assert.Equal(t, 5.5, rawData["value"])
	assert.Equal(t, 5.25, rawData["previous"])
	assert.Equal(t, "2024-01-15", rawData["date"])
	assert.Equal(t, true, rawData["has_previous"])
}

func TestStopProvider(t *testing.T) {
	cfg := DefaultConfig()
	cfg.APIKey = "test-key"
	cfg.PollInterval = 100 * time.Millisecond

	provider, err := New(cfg, zap.NewNop())
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err = provider.Start(ctx)
	require.NoError(t, err)

	time.Sleep(50 * time.Millisecond)

	err = provider.Stop()
	assert.NoError(t, err)
}
