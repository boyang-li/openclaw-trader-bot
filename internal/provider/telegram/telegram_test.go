package telegram

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
	assert.Equal(t, DefaultPollTimeout, cfg.PollTimeout)
	assert.True(t, cfg.Enabled)
	assert.NotEmpty(t, cfg.Keywords)
}

func TestNew_RequiresBotToken(t *testing.T) {
	cfg := DefaultConfig()
	cfg.BotToken = ""

	_, err := New(cfg, zap.NewNop())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "bot token is required")
}

func TestNew_Success(t *testing.T) {
	cfg := DefaultConfig()
	cfg.BotToken = "test-bot-token"

	p, err := New(cfg, zap.NewNop())
	require.NoError(t, err)
	require.NotNil(t, p)

	assert.Equal(t, "telegram", p.Name())
	assert.Equal(t, signal.CategoryGeopolitical, p.Category())
}

func TestNew_WithEmptyConfig(t *testing.T) {
	cfg := Config{
		BotToken: "test-token",
	}

	p, err := New(cfg, zap.NewNop())
	require.NoError(t, err)
	require.NotNil(t, p)

	// Should apply defaults
	assert.Equal(t, DefaultBaseURL, p.config.BaseURL)
	assert.Equal(t, DefaultPollInterval, p.config.PollInterval)
	assert.Equal(t, DefaultPollTimeout, p.config.PollTimeout)
}

func TestNew_WithChatFilters(t *testing.T) {
	cfg := DefaultConfig()
	cfg.BotToken = "test-token"
	cfg.ChatIDs = []int64{-1001234567890, -1009876543210}

	p, err := New(cfg, zap.NewNop())
	require.NoError(t, err)

	assert.True(t, p.allowedChats[-1001234567890])
	assert.True(t, p.allowedChats[-1009876543210])
	assert.False(t, p.allowedChats[-1000000000000])
}

func TestNew_WithNilLogger(t *testing.T) {
	cfg := DefaultConfig()
	cfg.BotToken = "test-token"

	p, err := New(cfg, nil)
	require.NoError(t, err)
	require.NotNil(t, p)
}

func TestClassifyMessage(t *testing.T) {
	tests := []struct {
		name     string
		text     string
		expected signal.Category
	}{
		{
			name:     "crypto message",
			text:     "Bitcoin whale alert: 10,000 BTC transferred to exchange",
			expected: signal.CategoryCrypto,
		},
		{
			name:     "macro message",
			text:     "Fed announces rate hike, inflation concerns grow",
			expected: signal.CategoryMacro,
		},
		{
			name:     "geopolitical message",
			text:     "Breaking: Military conflict escalates as missiles strike city",
			expected: signal.CategoryGeopolitical,
		},
		{
			name:     "mixed - crypto dominates",
			text:     "Ethereum price surges amid SEC ETF approval rumors",
			expected: signal.CategoryCrypto,
		},
		{
			name:     "mixed - geopolitical dominates",
			text:     "Russia-Ukraine war: NATO troops deploy to border",
			expected: signal.CategoryGeopolitical,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := classifyMessage(tt.text)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestDetermineMessageAction(t *testing.T) {
	tests := []struct {
		name     string
		text     string
		expected string
	}{
		{
			name:     "breaking news",
			text:     "BREAKING: Major announcement from the White House",
			expected: "breaking_news",
		},
		{
			name:     "military action",
			text:     "Missile strike reported in eastern region",
			expected: "military_action",
		},
		{
			name:     "sanction announcement",
			text:     "New sanctions imposed on foreign entities",
			expected: "sanction_announcement",
		},
		{
			name:     "rate decision",
			text:     "Federal Reserve announces rate hike of 25bps",
			expected: "rate_decision",
		},
		{
			name:     "security incident",
			text:     "Major DeFi protocol exploited, millions stolen",
			expected: "security_incident",
		},
		{
			name:     "market event",
			text:     "Massive liquidations as market crashes",
			expected: "market_event",
		},
		{
			name:     "generic update",
			text:     "Market update: Stocks mixed in afternoon trading",
			expected: "telegram_update",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := determineMessageAction(tt.text)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestAnalyzeMessageSentiment(t *testing.T) {
	tests := []struct {
		name         string
		text         string
		expectedSign float64
	}{
		{
			name:         "negative - attack",
			text:         "Attack on city causes widespread damage and fear",
			expectedSign: -1.0,
		},
		{
			name:         "negative - crash",
			text:         "Market crashes as crisis deepens",
			expectedSign: -1.0,
		},
		{
			name:         "positive - peace",
			text:         "Peace deal reached, markets rally on agreement",
			expectedSign: 1.0,
		},
		{
			name:         "positive - growth",
			text:         "Recovery continues with bullish growth outlook",
			expectedSign: 1.0,
		},
		{
			name:         "neutral",
			text:         "Markets closed for holiday",
			expectedSign: 0.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := analyzeMessageSentiment(tt.text)
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

func TestDetermineMessageUrgency(t *testing.T) {
	tests := []struct {
		name     string
		text     string
		expected signal.UrgencyLevel
	}{
		{
			name:     "breaking news",
			text:     "BREAKING: Major event unfolding",
			expected: signal.UrgencyHigh,
		},
		{
			name:     "urgent alert",
			text:     "URGENT: Immediate action required",
			expected: signal.UrgencyHigh,
		},
		{
			name:     "emergency",
			text:     "Emergency declared in multiple regions",
			expected: signal.UrgencyHigh,
		},
		{
			name:     "war-related",
			text:     "War intensifies as military operations expand",
			expected: signal.UrgencyHigh,
		},
		{
			name:     "attack news",
			text:     "Attack reported in major city",
			expected: signal.UrgencyHigh,
		},
		{
			name:     "developing story",
			text:     "Developing: Situation evolving rapidly",
			expected: signal.UrgencyMedium,
		},
		{
			name:     "just in",
			text:     "Just in: New information released",
			expected: signal.UrgencyMedium,
		},
		{
			name:     "regular update",
			text:     "Markets close higher on positive earnings",
			expected: signal.UrgencyLow,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := &Message{Text: tt.text}
			result := determineMessageUrgency(tt.text, msg)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestFormatMessageSource(t *testing.T) {
	tests := []struct {
		name     string
		msg      *Message
		expected string
	}{
		{
			name: "with chat title",
			msg: &Message{
				Chat: &Chat{
					Title:    "Breaking News Channel",
					Username: "newsbot",
				},
			},
			expected: "Breaking News Channel",
		},
		{
			name: "with chat username only",
			msg: &Message{
				Chat: &Chat{
					Username: "newsbot",
				},
			},
			expected: "@newsbot",
		},
		{
			name: "with from username",
			msg: &Message{
				Chat: &Chat{ID: 123},
				From: &User{Username: "john_doe"},
			},
			expected: "@john_doe",
		},
		{
			name: "fallback to chat id",
			msg: &Message{
				Chat: &Chat{ID: 123456},
			},
			expected: "chat_123456",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := formatMessageSource(tt.msg)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestFormatMessageURL(t *testing.T) {
	tests := []struct {
		name     string
		msg      *Message
		expected string
	}{
		{
			name: "with username",
			msg: &Message{
				Chat:      &Chat{Username: "newsbot"},
				MessageID: 12345,
			},
			expected: "https://t.me/newsbot/12345",
		},
		{
			name: "without username",
			msg: &Message{
				Chat:      &Chat{ID: -1001234567890},
				MessageID: 12345,
			},
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := formatMessageURL(tt.msg)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGenerateMessageTags(t *testing.T) {
	tests := []struct {
		name       string
		text       string
		msg        *Message
		expectTags []string
	}{
		{
			name: "channel with crypto content",
			text: "Bitcoin whale moves 1000 BTC",
			msg: &Message{
				Chat: &Chat{Type: "channel"},
			},
			expectTags: []string{"telegram", "channel", "crypto"},
		},
		{
			name: "group with geopolitical content",
			text: "Military operations in Ukraine continue",
			msg: &Message{
				Chat: &Chat{Type: "supergroup"},
			},
			expectTags: []string{"telegram", "group", "geopolitical"},
		},
		{
			name: "breaking news with macro content",
			text: "BREAKING: Fed announces rate cut",
			msg: &Message{
				Chat: &Chat{Type: "channel"},
			},
			expectTags: []string{"telegram", "channel", "macro", "breaking"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := generateMessageTags(tt.text, tt.msg)
			for _, tag := range tt.expectTags {
				assert.Contains(t, result, tag)
			}
		})
	}
}

func TestTruncateText(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		maxLen   int
		expected string
	}{
		{
			name:     "short text",
			input:    "Hello",
			maxLen:   10,
			expected: "Hello",
		},
		{
			name:     "exact length",
			input:    "HelloWorld",
			maxLen:   10,
			expected: "HelloWorld",
		},
		{
			name:     "needs truncation",
			input:    "Hello World, this is a long message",
			maxLen:   15,
			expected: "Hello World,...",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := truncateText(tt.input, tt.maxLen)
			assert.Equal(t, tt.expected, result)
			assert.LessOrEqual(t, len(result), tt.maxLen)
		})
	}
}

func TestGetUserID(t *testing.T) {
	assert.Equal(t, int64(0), getUserID(nil))
	assert.Equal(t, int64(12345), getUserID(&User{ID: 12345}))
}

func TestGetUsername(t *testing.T) {
	assert.Equal(t, "", getUsername(nil))
	assert.Equal(t, "john_doe", getUsername(&User{Username: "john_doe"}))
}

func TestGetMe_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Contains(t, r.URL.Path, "/getMe")

		response := apiResponse{
			OK: true,
			Result: json.RawMessage(`{
				"id": 123456789,
				"is_bot": true,
				"first_name": "Test Bot",
				"username": "test_bot"
			}`),
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	cfg := Config{
		BotToken: "test-token",
		BaseURL:  server.URL,
		Enabled:  true,
	}

	p, err := New(cfg, zap.NewNop())
	require.NoError(t, err)

	ctx := context.Background()
	user, err := p.getMe(ctx)

	require.NoError(t, err)
	assert.Equal(t, int64(123456789), user.ID)
	assert.True(t, user.IsBot)
	assert.Equal(t, "test_bot", user.Username)
}

func TestGetMe_APIError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		response := apiResponse{
			OK:          false,
			Description: "Unauthorized",
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	cfg := Config{
		BotToken: "invalid-token",
		BaseURL:  server.URL,
		Enabled:  true,
	}

	p, err := New(cfg, zap.NewNop())
	require.NoError(t, err)

	ctx := context.Background()
	_, err = p.getMe(ctx)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "API error")
}

func TestProcessUpdate_FiltersChat(t *testing.T) {
	cfg := DefaultConfig()
	cfg.BotToken = "test-token"
	cfg.ChatIDs = []int64{-1001111111111} // Only allow this chat

	p, err := New(cfg, zap.NewNop())
	require.NoError(t, err)

	var capturedSignals []signal.Signal
	p.Subscribe(func(ctx context.Context, sig signal.Signal) error {
		capturedSignals = append(capturedSignals, sig)
		return nil
	})

	// Message from non-allowed chat
	update := Update{
		UpdateID: 1,
		Message: &Message{
			MessageID: 1,
			Chat:      &Chat{ID: -1002222222222, Type: "channel"},
			Date:      time.Now().Unix(),
			Text:      "Breaking news about Bitcoin",
		},
	}

	ctx := context.Background()
	err = p.processUpdate(ctx, update)
	require.NoError(t, err)

	// Should not emit signal for non-allowed chat
	assert.Empty(t, capturedSignals)
}

func TestProcessUpdate_FiltersKeywords(t *testing.T) {
	cfg := DefaultConfig()
	cfg.BotToken = "test-token"
	cfg.Keywords = []string{"bitcoin", "breaking"}

	p, err := New(cfg, zap.NewNop())
	require.NoError(t, err)

	var capturedSignals []signal.Signal
	p.Subscribe(func(ctx context.Context, sig signal.Signal) error {
		capturedSignals = append(capturedSignals, sig)
		return nil
	})

	// Message without keywords
	update := Update{
		UpdateID: 1,
		Message: &Message{
			MessageID: 1,
			Chat:      &Chat{ID: -1001111111111, Type: "channel", Title: "News"},
			Date:      time.Now().Unix(),
			Text:      "Regular market update",
		},
	}

	ctx := context.Background()
	err = p.processUpdate(ctx, update)
	require.NoError(t, err)

	// Should not emit signal for message without keywords
	assert.Empty(t, capturedSignals)
}

func TestEmitMessageSignal(t *testing.T) {
	cfg := DefaultConfig()
	cfg.BotToken = "test-token"

	p, err := New(cfg, zap.NewNop())
	require.NoError(t, err)

	var capturedSignals []signal.Signal
	p.Subscribe(func(ctx context.Context, sig signal.Signal) error {
		capturedSignals = append(capturedSignals, sig)
		return nil
	})

	msg := &Message{
		MessageID: 12345,
		Chat: &Chat{
			ID:       -1001234567890,
			Type:     "channel",
			Title:    "Breaking News",
			Username: "breaking_news",
		},
		Date: time.Now().Unix(),
		From: &User{
			ID:       98765,
			Username: "news_bot",
		},
	}

	text := "BREAKING: Bitcoin surges past $100,000 as institutional adoption grows"

	ctx := context.Background()
	err = p.emitMessageSignal(ctx, msg, text)
	require.NoError(t, err)

	require.Len(t, capturedSignals, 1)
	sig := capturedSignals[0]

	assert.Equal(t, signal.SourceTelegram, sig.Source)
	assert.Equal(t, signal.CategoryCrypto, sig.Category)
	assert.Equal(t, "Breaking News", sig.Subject)
	assert.Equal(t, "breaking_news", sig.Action)
	assert.Contains(t, sig.Object, "Bitcoin")
	assert.Equal(t, signal.UrgencyHigh, sig.Urgency)
	assert.Contains(t, sig.Tags, "telegram")
	assert.Contains(t, sig.Tags, "channel")
	assert.Contains(t, sig.Tags, "crypto")
	assert.Contains(t, sig.Tags, "breaking")
}

func TestCleanupSeenMessages(t *testing.T) {
	cfg := DefaultConfig()
	cfg.BotToken = "test-token"

	p, err := New(cfg, zap.NewNop())
	require.NoError(t, err)

	// Add some messages
	p.seenMessages["old_msg"] = time.Now().Add(-2 * time.Hour) // Old (> 1 hour)
	p.seenMessages["new_msg"] = time.Now()                     // Recent

	p.cleanupSeenMessages()

	assert.NotContains(t, p.seenMessages, "old_msg")
	assert.Contains(t, p.seenMessages, "new_msg")
}
