// Package telegram provides a provider for Telegram channel/group message monitoring.
// It uses the Telegram Bot API to receive updates from subscribed channels.
package telegram

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/openclaworg/l1-ingestion/internal/provider"
	"github.com/openclaworg/l1-ingestion/internal/signal"
)

const (
	DefaultBaseURL      = "https://api.telegram.org"
	DefaultPollInterval = 5 * time.Second
	DefaultPollTimeout  = 30
	MaxMessageLength    = 4096
)

var DefaultKeywords = []string{
	"breaking", "urgent", "alert", "warning",
	"military", "attack", "strike", "explosion",
	"sanction", "tariff", "embargo",
	"fed", "ecb", "boj", "rate", "inflation",
	"bitcoin", "ethereum", "crypto", "whale",
}

var GeopoliticalKeywords = []string{
	"war", "conflict", "military", "troops", "missile",
	"nato", "russia", "ukraine", "china", "taiwan",
	"iran", "israel", "gaza", "hamas", "hezbollah",
	"nuclear", "sanction", "embargo", "coup",
}

var MacroKeywords = []string{
	"fed", "fomc", "ecb", "boj", "pboc", "rba",
	"rate hike", "rate cut", "inflation", "cpi", "gdp",
	"recession", "employment", "payroll", "unemployment",
	"treasury", "yield", "bond", "dollar", "euro",
}

var CryptoKeywords = []string{
	"bitcoin", "btc", "ethereum", "eth", "crypto",
	"whale", "liquidation", "hack", "exploit", "sec",
	"etf", "binance", "coinbase", "ftx", "defi",
}

type Config struct {
	BotToken     string
	BaseURL      string
	PollInterval time.Duration
	PollTimeout  int
	ChatIDs      []int64
	Keywords     []string
	Enabled      bool
}

func DefaultConfig() Config {
	return Config{
		BaseURL:      DefaultBaseURL,
		PollInterval: DefaultPollInterval,
		PollTimeout:  DefaultPollTimeout,
		Keywords:     DefaultKeywords,
		Enabled:      true,
	}
}

type Provider struct {
	*provider.BaseProvider

	config     Config
	httpClient *http.Client

	mu           sync.RWMutex
	lastUpdateID int64
	seenMessages map[string]time.Time
	keywordRegex *regexp.Regexp
	allowedChats map[int64]bool

	wg sync.WaitGroup
}

type Update struct {
	UpdateID      int64    `json:"update_id"`
	Message       *Message `json:"message"`
	ChannelPost   *Message `json:"channel_post"`
	EditedMessage *Message `json:"edited_message"`
}

type Message struct {
	MessageID   int64    `json:"message_id"`
	From        *User    `json:"from"`
	Chat        *Chat    `json:"chat"`
	Date        int64    `json:"date"`
	Text        string   `json:"text"`
	Caption     string   `json:"caption"`
	ForwardFrom *User    `json:"forward_from"`
	ForwardDate int64    `json:"forward_date"`
	ReplyTo     *Message `json:"reply_to_message"`
	Entities    []Entity `json:"entities"`
}

type User struct {
	ID        int64  `json:"id"`
	IsBot     bool   `json:"is_bot"`
	FirstName string `json:"first_name"`
	LastName  string `json:"last_name"`
	Username  string `json:"username"`
}

type Chat struct {
	ID        int64  `json:"id"`
	Type      string `json:"type"`
	Title     string `json:"title"`
	Username  string `json:"username"`
	FirstName string `json:"first_name"`
}

type Entity struct {
	Type   string `json:"type"`
	Offset int    `json:"offset"`
	Length int    `json:"length"`
	URL    string `json:"url"`
}

type apiResponse struct {
	OK          bool            `json:"ok"`
	Result      json.RawMessage `json:"result"`
	Description string          `json:"description"`
	ErrorCode   int             `json:"error_code"`
}

func New(cfg Config, logger *zap.Logger) (*Provider, error) {
	if cfg.BotToken == "" {
		return nil, fmt.Errorf("telegram: bot token is required")
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
	if cfg.PollTimeout == 0 {
		cfg.PollTimeout = DefaultPollTimeout
	}
	if len(cfg.Keywords) == 0 {
		cfg.Keywords = DefaultKeywords
	}

	allowedChats := make(map[int64]bool)
	for _, id := range cfg.ChatIDs {
		allowedChats[id] = true
	}

	keywordPattern := strings.Join(cfg.Keywords, "|")
	keywordRegex := regexp.MustCompile(`(?i)\b(` + keywordPattern + `)\b`)

	baseCfg := provider.ProviderConfig{
		Name:           "telegram",
		Category:       signal.CategoryGeopolitical,
		PollInterval:   cfg.PollInterval,
		RequestTimeout: time.Duration(cfg.PollTimeout+10) * time.Second,
		MaxRetries:     3,
		RetryBackoff:   5 * time.Second,
	}

	return &Provider{
		BaseProvider: provider.NewBaseProvider(baseCfg, logger.Named("telegram")),
		config:       cfg,
		httpClient: &http.Client{
			Timeout: time.Duration(cfg.PollTimeout+10) * time.Second,
		},
		seenMessages: make(map[string]time.Time),
		keywordRegex: keywordRegex,
		allowedChats: allowedChats,
	}, nil
}

func (p *Provider) Start(ctx context.Context) error {
	if !p.config.Enabled {
		p.Logger().Info("Telegram provider is disabled")
		return nil
	}

	me, err := p.getMe(ctx)
	if err != nil {
		return fmt.Errorf("validate bot token: %w", err)
	}

	p.Logger().Info("Telegram bot authenticated",
		zap.String("username", me.Username),
		zap.Int64("id", me.ID),
	)

	p.MarkStarted()

	p.wg.Add(1)
	go p.pollLoop(ctx)

	p.Logger().Info("Telegram provider started",
		zap.Duration("poll_interval", p.config.PollInterval),
		zap.Int("keywords", len(p.config.Keywords)),
		zap.Int("chat_filters", len(p.config.ChatIDs)),
	)

	return nil
}

func (p *Provider) Stop() error {
	err := p.BaseProvider.Stop()
	p.wg.Wait()
	p.Logger().Info("Telegram provider stopped")
	return err
}

func (p *Provider) getMe(ctx context.Context) (*User, error) {
	reqURL := fmt.Sprintf("%s/bot%s/getMe", p.config.BaseURL, p.config.BotToken)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, err
	}

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var apiResp apiResponse
	if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
		return nil, err
	}

	if !apiResp.OK {
		return nil, fmt.Errorf("API error: %s", apiResp.Description)
	}

	var user User
	if err := json.Unmarshal(apiResp.Result, &user); err != nil {
		return nil, err
	}

	return &user, nil
}

func (p *Provider) pollLoop(ctx context.Context) {
	defer p.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case <-p.StopChannel():
			return
		default:
		}

		updates, err := p.getUpdates(ctx)
		if err != nil {
			p.Logger().Warn("Failed to get updates", zap.Error(err))
			p.RecordError(err)
			time.Sleep(p.config.PollInterval)
			continue
		}

		for _, update := range updates {
			p.mu.Lock()
			if update.UpdateID > p.lastUpdateID {
				p.lastUpdateID = update.UpdateID
			}
			p.mu.Unlock()

			if err := p.processUpdate(ctx, update); err != nil {
				p.Logger().Warn("Failed to process update",
					zap.Error(err),
					zap.Int64("update_id", update.UpdateID),
				)
			}
		}

		p.cleanupSeenMessages()
	}
}

func (p *Provider) getUpdates(ctx context.Context) ([]Update, error) {
	p.mu.RLock()
	offset := p.lastUpdateID + 1
	p.mu.RUnlock()

	params := url.Values{}
	params.Set("offset", strconv.FormatInt(offset, 10))
	params.Set("timeout", strconv.Itoa(p.config.PollTimeout))
	params.Set("allowed_updates", `["message","channel_post"]`)

	reqURL := fmt.Sprintf("%s/bot%s/getUpdates?%s",
		p.config.BaseURL, p.config.BotToken, params.Encode())

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, err
	}

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var apiResp apiResponse
	if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
		return nil, err
	}

	if !apiResp.OK {
		return nil, fmt.Errorf("API error %d: %s", apiResp.ErrorCode, apiResp.Description)
	}

	var updates []Update
	if err := json.Unmarshal(apiResp.Result, &updates); err != nil {
		return nil, err
	}

	return updates, nil
}

func (p *Provider) processUpdate(ctx context.Context, update Update) error {
	var msg *Message
	if update.ChannelPost != nil {
		msg = update.ChannelPost
	} else if update.Message != nil {
		msg = update.Message
	} else {
		return nil
	}

	if len(p.allowedChats) > 0 && !p.allowedChats[msg.Chat.ID] {
		return nil
	}

	text := msg.Text
	if text == "" {
		text = msg.Caption
	}
	if text == "" {
		return nil
	}

	msgKey := fmt.Sprintf("%d_%d", msg.Chat.ID, msg.MessageID)
	p.mu.Lock()
	if _, seen := p.seenMessages[msgKey]; seen {
		p.mu.Unlock()
		return nil
	}
	p.seenMessages[msgKey] = time.Now()
	p.mu.Unlock()

	if !p.keywordRegex.MatchString(text) {
		return nil
	}

	return p.emitMessageSignal(ctx, msg, text)
}

func (p *Provider) emitMessageSignal(ctx context.Context, msg *Message, text string) error {
	category := classifyMessage(text)
	action := determineMessageAction(text)
	sentiment := analyzeMessageSentiment(text)
	urgency := determineMessageUrgency(text, msg)
	tags := generateMessageTags(text, msg)

	subject := formatMessageSource(msg)
	object := truncateText(text, 500)

	timestamp := time.Unix(msg.Date, 0)

	builder := signal.NewBuilder(signal.SourceTelegram, category).
		WithTimestamp(timestamp).
		WithSubject(subject).
		WithAction(action).
		WithObject(object).
		WithConfidence(0.7).
		WithSentiment(sentiment).
		WithUrgency(urgency).
		WithTags(tags...).
		WithProviderMeta(map[string]any{
			"source_id":  fmt.Sprintf("tg_%d_%d", msg.Chat.ID, msg.MessageID),
			"source_url": formatMessageURL(msg),
		}).
		WithRawData(map[string]interface{}{
			"chat_id":       msg.Chat.ID,
			"chat_type":     msg.Chat.Type,
			"chat_title":    msg.Chat.Title,
			"chat_username": msg.Chat.Username,
			"message_id":    msg.MessageID,
			"text":          truncateText(text, 2000),
			"date":          msg.Date,
			"from_id":       getUserID(msg.From),
			"from_username": getUsername(msg.From),
		})

	sig, err := builder.Build()
	if err != nil {
		return fmt.Errorf("build signal: %w", err)
	}

	if err := p.Emit(ctx, sig); err != nil {
		return fmt.Errorf("emit signal: %w", err)
	}

	p.Logger().Info("Telegram message processed",
		zap.String("chat", msg.Chat.Title),
		zap.String("category", string(category)),
		zap.String("action", action),
		zap.Int("text_length", len(text)),
	)

	return nil
}

func classifyMessage(text string) signal.Category {
	textLower := strings.ToLower(text)

	cryptoScore := 0
	for _, kw := range CryptoKeywords {
		if strings.Contains(textLower, kw) {
			cryptoScore++
		}
	}

	macroScore := 0
	for _, kw := range MacroKeywords {
		if strings.Contains(textLower, kw) {
			macroScore++
		}
	}

	geoScore := 0
	for _, kw := range GeopoliticalKeywords {
		if strings.Contains(textLower, kw) {
			geoScore++
		}
	}

	if cryptoScore > macroScore && cryptoScore > geoScore {
		return signal.CategoryCrypto
	}
	if macroScore > geoScore {
		return signal.CategoryMacro
	}
	return signal.CategoryGeopolitical
}

func determineMessageAction(text string) string {
	textLower := strings.ToLower(text)

	patterns := []struct {
		keywords []string
		action   string
	}{
		{[]string{"breaking", "urgent", "alert"}, "breaking_news"},
		{[]string{"attack", "strike", "bomb", "missile"}, "military_action"},
		{[]string{"sanction", "embargo", "ban"}, "sanction_announcement"},
		{[]string{"rate hike", "rate cut", "rate decision"}, "rate_decision"},
		{[]string{"hack", "exploit", "breach"}, "security_incident"},
		{[]string{"liquidat", "dump", "crash"}, "market_event"},
		{[]string{"announce", "statement", "press"}, "official_statement"},
		{[]string{"election", "vote", "poll"}, "political_event"},
		{[]string{"protest", "riot", "demonstration"}, "civil_unrest"},
	}

	for _, p := range patterns {
		for _, kw := range p.keywords {
			if strings.Contains(textLower, kw) {
				return p.action
			}
		}
	}

	return "telegram_update"
}

func analyzeMessageSentiment(text string) float64 {
	textLower := strings.ToLower(text)

	negativeWords := []string{
		"attack", "war", "crash", "collapse", "crisis", "fear",
		"hack", "exploit", "breach", "loss", "drop", "plunge",
		"threat", "danger", "warning", "alert", "emergency",
		"sanction", "ban", "embargo", "conflict", "death", "kill",
	}

	positiveWords := []string{
		"peace", "deal", "agreement", "rally", "surge", "gain",
		"recovery", "growth", "bullish", "breakthrough", "success",
		"approve", "support", "alliance", "cooperation",
	}

	score := 0.0
	for _, word := range negativeWords {
		if strings.Contains(textLower, word) {
			score -= 0.15
		}
	}
	for _, word := range positiveWords {
		if strings.Contains(textLower, word) {
			score += 0.15
		}
	}

	if score > 1.0 {
		score = 1.0
	} else if score < -1.0 {
		score = -1.0
	}

	return score
}

func determineMessageUrgency(text string, msg *Message) signal.UrgencyLevel {
	textLower := strings.ToLower(text)

	criticalKeywords := []string{
		"breaking", "urgent", "emergency", "alert", "war",
		"attack", "missile", "nuclear", "explosion",
	}
	for _, kw := range criticalKeywords {
		if strings.Contains(textLower, kw) {
			return signal.UrgencyHigh
		}
	}

	if strings.Contains(textLower, "just in") ||
		strings.Contains(textLower, "developing") ||
		strings.Contains(textLower, "happening now") {
		return signal.UrgencyMedium
	}

	return signal.UrgencyLow
}

func formatMessageSource(msg *Message) string {
	if msg.Chat.Title != "" {
		return msg.Chat.Title
	}
	if msg.Chat.Username != "" {
		return "@" + msg.Chat.Username
	}
	if msg.From != nil && msg.From.Username != "" {
		return "@" + msg.From.Username
	}
	return fmt.Sprintf("chat_%d", msg.Chat.ID)
}

func formatMessageURL(msg *Message) string {
	if msg.Chat.Username != "" {
		return fmt.Sprintf("https://t.me/%s/%d", msg.Chat.Username, msg.MessageID)
	}
	return ""
}

func generateMessageTags(text string, msg *Message) []string {
	tags := []string{"telegram"}

	if msg.Chat.Type == "channel" {
		tags = append(tags, "channel")
	} else if msg.Chat.Type == "supergroup" || msg.Chat.Type == "group" {
		tags = append(tags, "group")
	}

	textLower := strings.ToLower(text)

	for _, kw := range CryptoKeywords {
		if strings.Contains(textLower, kw) {
			tags = append(tags, "crypto")
			break
		}
	}

	for _, kw := range MacroKeywords {
		if strings.Contains(textLower, kw) {
			tags = append(tags, "macro")
			break
		}
	}

	for _, kw := range GeopoliticalKeywords {
		if strings.Contains(textLower, kw) {
			tags = append(tags, "geopolitical")
			break
		}
	}

	if strings.Contains(textLower, "breaking") || strings.Contains(textLower, "urgent") {
		tags = append(tags, "breaking")
	}

	return tags
}

func truncateText(text string, maxLen int) string {
	if len(text) <= maxLen {
		return text
	}
	return text[:maxLen-3] + "..."
}

func getUserID(user *User) int64 {
	if user == nil {
		return 0
	}
	return user.ID
}

func getUsername(user *User) string {
	if user == nil {
		return ""
	}
	return user.Username
}

func (p *Provider) cleanupSeenMessages() {
	p.mu.Lock()
	defer p.mu.Unlock()

	cutoff := time.Now().Add(-1 * time.Hour)
	for key, seenAt := range p.seenMessages {
		if seenAt.Before(cutoff) {
			delete(p.seenMessages, key)
		}
	}
}
