// Package whalealert provides a provider for Whale Alert cryptocurrency transaction monitoring.
// It polls the Whale Alert API for large cryptocurrency transfers across various blockchains.
package whalealert

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/openclaworg/l1-ingestion/internal/provider"
	"github.com/openclaworg/l1-ingestion/internal/signal"
)

const (
	DefaultBaseURL       = "https://api.whale-alert.io/v1"
	DefaultPollInterval  = 1 * time.Minute
	DefaultMinValueUSD   = 1_000_000 // $1M minimum
	DefaultLookbackMins  = 5         // Look back 5 minutes each poll
	MaxRequestsPerMinute = 10        // Free tier limit
)

// Config holds Whale Alert specific configuration.
type Config struct {
	APIKey       string
	BaseURL      string
	PollInterval time.Duration
	MinValueUSD  int64
	Enabled      bool

	// Blockchain filters (empty means all)
	Blockchains []string

	// Transaction type filters (empty means all)
	// Types: transfer, mint, burn, lock, unlock
	TransactionTypes []string
}

// DefaultConfig returns sensible defaults for Whale Alert.
func DefaultConfig() Config {
	return Config{
		BaseURL:          DefaultBaseURL,
		PollInterval:     DefaultPollInterval,
		MinValueUSD:      DefaultMinValueUSD,
		Enabled:          true,
		Blockchains:      []string{}, // All blockchains
		TransactionTypes: []string{}, // All types
	}
}

// Provider implements the Whale Alert data provider.
type Provider struct {
	*provider.BaseProvider

	config     Config
	httpClient *http.Client

	mu          sync.RWMutex
	lastCursor  string
	lastPollAt  time.Time
	seenTxIDs   map[string]time.Time // Track seen transactions to avoid duplicates
	rateLimiter *time.Ticker

	wg sync.WaitGroup
}

// New creates a new Whale Alert provider instance.
func New(cfg Config, logger *zap.Logger) (*Provider, error) {
	if cfg.APIKey == "" {
		return nil, fmt.Errorf("whale alert: API key is required")
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
	if cfg.MinValueUSD == 0 {
		cfg.MinValueUSD = DefaultMinValueUSD
	}

	baseCfg := provider.ProviderConfig{
		Name:           "whale_alert",
		Category:       signal.CategoryCrypto,
		PollInterval:   cfg.PollInterval,
		RequestTimeout: 30 * time.Second,
		MaxRetries:     3,
		RetryBackoff:   5 * time.Second,
	}

	return &Provider{
		BaseProvider: provider.NewBaseProvider(baseCfg, logger.Named("whale_alert")),
		config:       cfg,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		seenTxIDs:   make(map[string]time.Time),
		rateLimiter: time.NewTicker(time.Minute / MaxRequestsPerMinute),
	}, nil
}

// Start begins polling the Whale Alert API.
func (p *Provider) Start(ctx context.Context) error {
	if !p.config.Enabled {
		p.Logger().Info("Whale Alert provider is disabled")
		return nil
	}

	p.MarkStarted()

	p.wg.Add(1)
	go p.pollLoop(ctx)

	p.Logger().Info("Whale Alert provider started",
		zap.Duration("poll_interval", p.config.PollInterval),
		zap.Int64("min_value_usd", p.config.MinValueUSD),
		zap.Strings("blockchains", p.config.Blockchains),
	)

	return nil
}

// Stop gracefully stops the provider.
func (p *Provider) Stop() error {
	err := p.BaseProvider.Stop()

	p.rateLimiter.Stop()
	p.wg.Wait()

	p.Logger().Info("Whale Alert provider stopped")
	return err
}

// pollLoop continuously polls the Whale Alert API.
func (p *Provider) pollLoop(ctx context.Context) {
	defer p.wg.Done()

	ticker := time.NewTicker(p.config.PollInterval)
	defer ticker.Stop()

	// Initial poll
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

// poll fetches recent transactions from Whale Alert.
func (p *Provider) poll(ctx context.Context) error {
	// Rate limiting
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.rateLimiter.C:
	}

	// Calculate time window
	now := time.Now()
	startTime := now.Add(-time.Duration(DefaultLookbackMins) * time.Minute)

	transactions, cursor, err := p.fetchTransactions(ctx, startTime, "")
	if err != nil {
		return fmt.Errorf("fetch transactions: %w", err)
	}

	// Process transactions
	for _, tx := range transactions {
		// Skip already seen transactions
		p.mu.Lock()
		if _, seen := p.seenTxIDs[tx.ID]; seen {
			p.mu.Unlock()
			continue
		}
		p.seenTxIDs[tx.ID] = time.Now()
		p.mu.Unlock()

		// Apply filters
		if !p.passesFilters(tx) {
			continue
		}

		// Emit signal
		if err := p.emitTransactionSignal(ctx, tx); err != nil {
			p.Logger().Warn("Failed to emit signal", zap.Error(err), zap.String("tx_id", tx.ID))
		}
	}

	// Update cursor for pagination (if needed in future)
	p.mu.Lock()
	p.lastCursor = cursor
	p.lastPollAt = now
	p.mu.Unlock()

	// Cleanup old seen transaction IDs (keep last hour)
	p.cleanupSeenTxs()

	return nil
}

// Transaction represents a Whale Alert transaction.
type Transaction struct {
	ID              string  `json:"id"`
	Blockchain      string  `json:"blockchain"`
	Symbol          string  `json:"symbol"`
	TransactionType string  `json:"transaction_type"`
	Hash            string  `json:"hash"`
	From            Owner   `json:"from"`
	To              Owner   `json:"to"`
	Timestamp       int64   `json:"timestamp"`
	Amount          float64 `json:"amount"`
	AmountUSD       float64 `json:"amount_usd"`
}

// Owner represents a wallet owner in a transaction.
type Owner struct {
	Address   string `json:"address"`
	Owner     string `json:"owner"`
	OwnerType string `json:"owner_type"`
}

// apiResponse represents the Whale Alert API response.
type apiResponse struct {
	Result       string        `json:"result"`
	Cursor       string        `json:"cursor"`
	Count        int           `json:"count"`
	Transactions []Transaction `json:"transactions"`
	Message      string        `json:"message,omitempty"`
}

// fetchTransactions retrieves transactions from the API.
func (p *Provider) fetchTransactions(ctx context.Context, start time.Time, cursor string) ([]Transaction, string, error) {
	params := url.Values{}
	params.Set("api_key", p.config.APIKey)
	params.Set("min_value", strconv.FormatInt(p.config.MinValueUSD, 10))
	params.Set("start", strconv.FormatInt(start.Unix(), 10))

	if cursor != "" {
		params.Set("cursor", cursor)
	}

	reqURL := fmt.Sprintf("%s/transactions?%s", p.config.BaseURL, params.Encode())

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, "", fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "ACC-L1-Ingestion/1.0")

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return nil, "", fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		var errResp apiResponse
		json.NewDecoder(resp.Body).Decode(&errResp)
		return nil, "", fmt.Errorf("API error %d: %s", resp.StatusCode, errResp.Message)
	}

	var apiResp apiResponse
	if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
		return nil, "", fmt.Errorf("decode response: %w", err)
	}

	if apiResp.Result != "success" {
		return nil, "", fmt.Errorf("API returned non-success: %s", apiResp.Message)
	}

	p.Logger().Debug("Fetched transactions",
		zap.Int("count", apiResp.Count),
		zap.String("cursor", apiResp.Cursor),
	)

	return apiResp.Transactions, apiResp.Cursor, nil
}

// passesFilters checks if a transaction passes the configured filters.
func (p *Provider) passesFilters(tx Transaction) bool {
	// Check blockchain filter
	if len(p.config.Blockchains) > 0 {
		found := false
		for _, bc := range p.config.Blockchains {
			if strings.EqualFold(tx.Blockchain, bc) {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	// Check transaction type filter
	if len(p.config.TransactionTypes) > 0 {
		found := false
		for _, tt := range p.config.TransactionTypes {
			if strings.EqualFold(tx.TransactionType, tt) {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	return true
}

// emitTransactionSignal creates and emits a signal for a whale transaction.
func (p *Provider) emitTransactionSignal(ctx context.Context, tx Transaction) error {
	// Determine action based on transaction type
	action := mapTransactionAction(tx.TransactionType)

	// Determine sentiment based on transaction type and parties
	sentiment := calculateSentiment(tx)

	// Determine urgency based on USD value
	urgency := calculateUrgency(tx.AmountUSD)

	// Build subject (who initiated)
	subject := buildSubject(tx)

	// Build object (recipient or target)
	object := buildObject(tx)

	// Generate tags
	tags := generateTags(tx)

	builder := signal.NewBuilder(signal.SourceWhaleAlert, signal.CategoryCrypto).
		WithTimestamp(time.Unix(tx.Timestamp, 0)).
		WithSubject(subject).
		WithAction(action).
		WithObject(object).
		WithConfidence(0.99). // Direct blockchain data
		WithSentiment(sentiment).
		WithUrgency(urgency).
		WithTags(tags...).
		WithProviderMeta(map[string]any{
			"source_id":  tx.ID,
			"source_url": fmt.Sprintf("https://whale-alert.io/transaction/%s/%s", tx.Blockchain, tx.Hash),
		}).
		WithRawData(map[string]interface{}{
			"blockchain":       tx.Blockchain,
			"symbol":           tx.Symbol,
			"transaction_type": tx.TransactionType,
			"hash":             tx.Hash,
			"amount":           tx.Amount,
			"amount_usd":       tx.AmountUSD,
			"from_address":     tx.From.Address,
			"from_owner":       tx.From.Owner,
			"from_owner_type":  tx.From.OwnerType,
			"to_address":       tx.To.Address,
			"to_owner":         tx.To.Owner,
			"to_owner_type":    tx.To.OwnerType,
		})

	sig, err := builder.Build()
	if err != nil {
		return fmt.Errorf("build signal: %w", err)
	}

	if err := p.Emit(ctx, sig); err != nil {
		return fmt.Errorf("emit signal: %w", err)
	}

	p.Logger().Info("Whale transaction detected",
		zap.String("tx_id", tx.ID),
		zap.String("blockchain", tx.Blockchain),
		zap.String("symbol", tx.Symbol),
		zap.String("type", tx.TransactionType),
		zap.Float64("amount", tx.Amount),
		zap.Float64("amount_usd", tx.AmountUSD),
		zap.String("from", formatOwner(tx.From)),
		zap.String("to", formatOwner(tx.To)),
	)

	return nil
}

// mapTransactionAction maps Whale Alert transaction types to signal actions.
func mapTransactionAction(txType string) string {
	switch strings.ToLower(txType) {
	case "transfer":
		return "whale_transfer"
	case "mint":
		return "whale_mint"
	case "burn":
		return "whale_burn"
	case "lock":
		return "whale_lock"
	case "unlock":
		return "whale_unlock"
	default:
		return "whale_" + strings.ToLower(txType)
	}
}

// calculateSentiment determines sentiment based on transaction characteristics.
func calculateSentiment(tx Transaction) float64 {
	// Exchange deposits often indicate selling pressure (negative)
	// Exchange withdrawals indicate accumulation (positive)
	// Burns are often positive (supply reduction)
	// Mints can be negative (supply increase)

	fromType := strings.ToLower(tx.From.OwnerType)
	toType := strings.ToLower(tx.To.OwnerType)

	switch strings.ToLower(tx.TransactionType) {
	case "burn":
		return 0.5 // Supply reduction is generally bullish

	case "mint":
		return -0.3 // Supply increase is generally bearish

	case "transfer":
		// Exchange deposit = potential sell
		if toType == "exchange" && fromType != "exchange" {
			return -0.4
		}
		// Exchange withdrawal = accumulation
		if fromType == "exchange" && toType != "exchange" {
			return 0.4
		}
		// Exchange to exchange = neutral
		if fromType == "exchange" && toType == "exchange" {
			return 0.0
		}
		// Unknown wallet to unknown wallet = neutral
		return 0.0

	case "lock":
		return 0.3 // Locking is generally bullish (reducing supply)

	case "unlock":
		return -0.2 // Unlocking may indicate future selling

	default:
		return 0.0
	}
}

// calculateUrgency determines urgency based on USD value.
func calculateUrgency(amountUSD float64) signal.UrgencyLevel {
	switch {
	case amountUSD >= 100_000_000: // $100M+
		return signal.UrgencyCritical
	case amountUSD >= 50_000_000: // $50M+
		return signal.UrgencyHigh
	case amountUSD >= 10_000_000: // $10M+
		return signal.UrgencyMedium
	default:
		return signal.UrgencyLow
	}
}

// buildSubject creates a human-readable subject for the signal.
func buildSubject(tx Transaction) string {
	if tx.From.Owner != "" {
		return tx.From.Owner
	}
	if tx.From.OwnerType != "" {
		return fmt.Sprintf("%s (%s)", tx.From.OwnerType, shortenAddress(tx.From.Address))
	}
	return shortenAddress(tx.From.Address)
}

// buildObject creates a human-readable object for the signal.
func buildObject(tx Transaction) string {
	switch strings.ToLower(tx.TransactionType) {
	case "burn":
		return fmt.Sprintf("%.2f %s (burned)", tx.Amount, strings.ToUpper(tx.Symbol))
	case "mint":
		return fmt.Sprintf("%.2f %s (minted)", tx.Amount, strings.ToUpper(tx.Symbol))
	default:
		if tx.To.Owner != "" {
			return tx.To.Owner
		}
		if tx.To.OwnerType != "" {
			return fmt.Sprintf("%s (%s)", tx.To.OwnerType, shortenAddress(tx.To.Address))
		}
		return shortenAddress(tx.To.Address)
	}
}

// generateTags creates relevant tags for the transaction.
func generateTags(tx Transaction) []string {
	tags := []string{
		"whale",
		strings.ToLower(tx.Blockchain),
		strings.ToLower(tx.Symbol),
		strings.ToLower(tx.TransactionType),
	}

	// Add value tier tag
	switch {
	case tx.AmountUSD >= 100_000_000:
		tags = append(tags, "mega-whale", ">$100M")
	case tx.AmountUSD >= 50_000_000:
		tags = append(tags, "large-whale", ">$50M")
	case tx.AmountUSD >= 10_000_000:
		tags = append(tags, ">$10M")
	case tx.AmountUSD >= 1_000_000:
		tags = append(tags, ">$1M")
	}

	// Add flow direction tags
	fromType := strings.ToLower(tx.From.OwnerType)
	toType := strings.ToLower(tx.To.OwnerType)

	if toType == "exchange" && fromType != "exchange" {
		tags = append(tags, "exchange-inflow")
	}
	if fromType == "exchange" && toType != "exchange" {
		tags = append(tags, "exchange-outflow")
	}

	// Add known entity tags
	if tx.From.Owner != "" {
		tags = append(tags, fmt.Sprintf("from:%s", strings.ToLower(strings.ReplaceAll(tx.From.Owner, " ", "-"))))
	}
	if tx.To.Owner != "" {
		tags = append(tags, fmt.Sprintf("to:%s", strings.ToLower(strings.ReplaceAll(tx.To.Owner, " ", "-"))))
	}

	return tags
}

// shortenAddress shortens a blockchain address for display.
func shortenAddress(addr string) string {
	if len(addr) <= 13 {
		return addr
	}
	return addr[:6] + "..." + addr[len(addr)-4:]
}

// formatOwner formats an owner for logging.
func formatOwner(o Owner) string {
	if o.Owner != "" {
		return o.Owner
	}
	if o.OwnerType != "" {
		return fmt.Sprintf("%s (%s)", o.OwnerType, shortenAddress(o.Address))
	}
	return shortenAddress(o.Address)
}

// cleanupSeenTxs removes old entries from the seen transactions map.
func (p *Provider) cleanupSeenTxs() {
	p.mu.Lock()
	defer p.mu.Unlock()

	cutoff := time.Now().Add(-1 * time.Hour)
	for id, seenAt := range p.seenTxIDs {
		if seenAt.Before(cutoff) {
			delete(p.seenTxIDs, id)
		}
	}
}
