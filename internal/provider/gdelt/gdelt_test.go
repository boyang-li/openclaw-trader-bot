package gdelt

import (
	"strings"
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
	assert.Equal(t, "gdelt", provider.Name())
	assert.Equal(t, signal.CategoryGeopolitical, provider.Category())
}

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	assert.Equal(t, DefaultBaseURL, cfg.BaseURL)
	assert.Equal(t, DefaultPollInterval, cfg.PollInterval)
	assert.Equal(t, 250, cfg.BatchSize)
	assert.True(t, cfg.Enabled)
}

func TestGetLatestFileTime(t *testing.T) {
	cfg := DefaultConfig()
	provider := New(cfg, zap.NewNop())

	fileTime := provider.getLatestFileTime()

	assert.True(t, fileTime.Before(time.Now()))
	assert.Equal(t, 0, fileTime.Second())
	assert.True(t, fileTime.Minute()%15 == 0)
}

func TestBuildGKGURL(t *testing.T) {
	cfg := DefaultConfig()
	provider := New(cfg, zap.NewNop())

	testTime := time.Date(2024, 1, 15, 14, 30, 0, 0, time.UTC)
	url := provider.buildGKGURL(testTime)

	assert.Contains(t, url, "20240115143000.gkg.csv.zip")
	assert.Contains(t, url, DefaultBaseURL)
}

func TestParseTone(t *testing.T) {
	cfg := DefaultConfig()
	provider := New(cfg, zap.NewNop())

	tests := []struct {
		input    string
		expected float64
	}{
		{"-5.2,10.5,20.3", -5.2},
		{"3.5,5.0,10.0", 3.5},
		{"", 0},
		{"invalid", 0},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := provider.parseTone(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestParseThemes(t *testing.T) {
	cfg := DefaultConfig()
	provider := New(cfg, zap.NewNop())

	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name:     "multiple themes",
			input:    "MILITARY,100;PROTEST,200;CONFLICT,300",
			expected: []string{"military", "protest", "conflict"},
		},
		{
			name:     "empty",
			input:    "",
			expected: nil,
		},
		{
			name:     "single theme",
			input:    "TRADE,50",
			expected: []string{"trade"},
		},
		{
			name:     "duplicate themes",
			input:    "MILITARY,100;MILITARY,200",
			expected: []string{"military"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := provider.parseThemes(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestParseLocations(t *testing.T) {
	cfg := DefaultConfig()
	provider := New(cfg, zap.NewNop())

	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name:     "multiple locations",
			input:    "1#Beijing#CH#00#00#39.9#116.4#123;2#Washington#US#DC#00#38.9#-77.0#456",
			expected: []string{"CH", "US"},
		},
		{
			name:     "empty",
			input:    "",
			expected: nil,
		},
		{
			name:     "single location",
			input:    "1#London#UK#ENG#00#51.5#-0.1#789",
			expected: []string{"UK"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := provider.parseLocations(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestParseEntities(t *testing.T) {
	cfg := DefaultConfig()
	provider := New(cfg, zap.NewNop())

	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name:     "multiple entities",
			input:    "United Nations,100;World Bank,200",
			expected: []string{"United Nations", "World Bank"},
		},
		{
			name:     "empty",
			input:    "",
			expected: nil,
		},
		{
			name:     "with duplicates",
			input:    "NATO,100;NATO,200;EU,300",
			expected: []string{"NATO", "EU"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := provider.parseEntities(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestDetermineAction(t *testing.T) {
	cfg := DefaultConfig()
	provider := New(cfg, zap.NewNop())

	tests := []struct {
		themes   []string
		expected string
	}{
		{[]string{"military", "conflict"}, "military_activity"},
		{[]string{"protest", "civil_unrest"}, "civil_unrest"},
		{[]string{"terror", "attack"}, "security_incident"},
		{[]string{"election", "vote"}, "political_event"},
		{[]string{"unknown_theme"}, "geopolitical_event"},
		{[]string{}, "geopolitical_event"},
	}

	for _, tt := range tests {
		t.Run(strings.Join(tt.themes, ","), func(t *testing.T) {
			result := provider.determineAction(tt.themes)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestDetermineUrgency(t *testing.T) {
	cfg := DefaultConfig()
	provider := New(cfg, zap.NewNop())

	tests := []struct {
		name     string
		tone     float64
		themes   []string
		expected signal.UrgencyLevel
	}{
		{"very negative tone", -7.0, []string{"economy"}, signal.UrgencyHigh},
		{"military theme", -2.0, []string{"military"}, signal.UrgencyMedium},
		{"terror theme", 0.0, []string{"terror"}, signal.UrgencyMedium},
		{"neutral", 0.0, []string{"trade"}, signal.UrgencyLow},
		{"positive", 5.0, []string{"diplomacy"}, signal.UrgencyLow},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := provider.determineUrgency(tt.tone, tt.themes)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestCalculateConfidence(t *testing.T) {
	cfg := DefaultConfig()
	provider := New(cfg, zap.NewNop())

	tests := []struct {
		name     string
		source   string
		expected float64
	}{
		{"reuters", "reuters.com", 0.8},
		{"bbc", "bbc.co.uk", 0.8},
		{"unknown", "random-blog.com", 0.6},
		{"ap news", "apnews.com", 0.8},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fields := make([]string, 10)
			fields[4] = tt.source
			result := provider.calculateConfidence(fields)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestHasMatchingTheme(t *testing.T) {
	cfg := DefaultConfig()
	cfg.FilterThemes = []string{"military", "protest"}
	provider := New(cfg, zap.NewNop())

	tests := []struct {
		themes   []string
		expected bool
	}{
		{[]string{"military", "conflict"}, true},
		{[]string{"protest_march"}, true},
		{[]string{"trade", "economy"}, false},
		{[]string{}, false},
	}

	for _, tt := range tests {
		t.Run(strings.Join(tt.themes, ","), func(t *testing.T) {
			result := provider.hasMatchingTheme(tt.themes)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestHasMatchingCountry(t *testing.T) {
	cfg := DefaultConfig()
	cfg.FilterCountries = []string{"US", "CN", "RU"}
	provider := New(cfg, zap.NewNop())

	tests := []struct {
		locations []string
		expected  bool
	}{
		{[]string{"US", "UK"}, true},
		{[]string{"CN"}, true},
		{[]string{"JP", "KR"}, false},
		{[]string{}, false},
	}

	for _, tt := range tests {
		t.Run(strings.Join(tt.locations, ","), func(t *testing.T) {
			result := provider.hasMatchingCountry(tt.locations)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestParseGKGLine(t *testing.T) {
	cfg := DefaultConfig()
	provider := New(cfg, zap.NewNop())

	validLine := strings.Join([]string{
		"20240115143000-T123",               // 0: GKGRECORDID
		"20240115143000",                    // 1: DATE
		"",                                  // 2
		"1",                                 // 3: SourceCollectionIdentifier
		"reuters.com",                       // 4: SourceCommonName
		"",                                  // 5
		"",                                  // 6
		"MILITARY,100;CONFLICT,200",         // 7: Themes
		"",                                  // 8
		"1#Beijing#CN#00#00#39.9#116.4#123", // 9: Locations
		"",                                  // 10
		"Xi Jinping,100",                    // 11: Persons
		"",                                  // 12
		"PLA,100;United Nations,200",        // 13: Organizations
		"",                                  // 14
		"-3.5,5.0,10.0",                     // 15: Tone
		"",                                  // 16
		"",                                  // 17: GCAM
		"", "", "", "", "", "", "", "", "",  // 18-26: padding
	}, "\t")

	fileTime := time.Date(2024, 1, 15, 14, 30, 0, 0, time.UTC)

	sig, ok := provider.parseGKGLine(validLine, fileTime)

	require.True(t, ok)
	assert.Equal(t, "CN", sig.Subject)
	assert.Equal(t, "military_activity", sig.Action)
	assert.Equal(t, signal.SourceGDELT, sig.Source)
	assert.Equal(t, signal.CategoryGeopolitical, sig.Category)
	assert.Equal(t, 0.8, sig.Confidence) // reuters = reliable
	assert.True(t, sig.Sentiment < 0)    // negative tone
}

func TestParseGKGLine_InvalidLine(t *testing.T) {
	cfg := DefaultConfig()
	provider := New(cfg, zap.NewNop())

	shortLine := "field1\tfield2\tfield3"
	fileTime := time.Now()

	_, ok := provider.parseGKGLine(shortLine, fileTime)
	assert.False(t, ok)
}

func TestDetermineSubject(t *testing.T) {
	cfg := DefaultConfig()
	provider := New(cfg, zap.NewNop())

	tests := []struct {
		name      string
		locations []string
		orgs      []string
		persons   []string
		expected  string
	}{
		{"location first", []string{"US"}, []string{"NATO"}, []string{"Biden"}, "US"},
		{"org when no location", nil, []string{"NATO"}, []string{"Biden"}, "NATO"},
		{"person when no others", nil, nil, []string{"Biden"}, "Biden"},
		{"empty", nil, nil, nil, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := provider.determineSubject(tt.locations, tt.orgs, tt.persons)
			assert.Equal(t, tt.expected, result)
		})
	}
}
