// Package cot provides a provider for CFTC Commitments of Traders (COT) reports.
// It fetches weekly futures positioning data from the CFTC website.
package cot

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/openclaworg/l1-ingestion/internal/provider"
	"github.com/openclaworg/l1-ingestion/internal/signal"
)

const (
	DefaultBaseURL      = "https://www.cftc.gov/files/dea/history"
	DefaultPollInterval = 6 * time.Hour
	COTReleaseDay       = time.Friday
	COTReleaseHour      = 15
	COTReleaseMinute    = 30
)

var DefaultContracts = []string{
	"GOLD - COMMODITY EXCHANGE INC.",
	"SILVER - COMMODITY EXCHANGE INC.",
	"WTI CRUDE OIL - NEW YORK MERCANTILE EXCHANGE",
	"NATURAL GAS - NEW YORK MERCANTILE EXCHANGE",
	"E-MINI S&P 500 - CHICAGO MERCANTILE EXCHANGE",
	"NASDAQ MINI - CHICAGO MERCANTILE EXCHANGE",
	"U.S. DOLLAR INDEX - ICE FUTURES U.S.",
	"EURO FX - CHICAGO MERCANTILE EXCHANGE",
	"JAPANESE YEN - CHICAGO MERCANTILE EXCHANGE",
	"BITCOIN - CHICAGO MERCANTILE EXCHANGE",
}

type Config struct {
	BaseURL      string
	PollInterval time.Duration
	Contracts    []string
	Enabled      bool
}

func DefaultConfig() Config {
	return Config{
		BaseURL:      DefaultBaseURL,
		PollInterval: DefaultPollInterval,
		Contracts:    DefaultContracts,
		Enabled:      true,
	}
}

type Provider struct {
	*provider.BaseProvider

	config     Config
	httpClient *http.Client

	mu              sync.RWMutex
	lastReportDate  time.Time
	previousData    map[string]*COTData
	contractFilters map[string]bool

	wg sync.WaitGroup
}

type COTData struct {
	ContractName       string
	ReportDate         time.Time
	OpenInterest       int64
	NonCommLong        int64
	NonCommShort       int64
	NonCommSpreads     int64
	CommLong           int64
	CommShort          int64
	NonReportableLong  int64
	NonReportableShort int64
	NetNonComm         int64
	NetComm            int64
	PercentNonCommLong float64
	PercentCommLong    float64
}

func New(cfg Config, logger *zap.Logger) *Provider {
	if logger == nil {
		logger = zap.NewNop()
	}

	if cfg.BaseURL == "" {
		cfg.BaseURL = DefaultBaseURL
	}
	if cfg.PollInterval == 0 {
		cfg.PollInterval = DefaultPollInterval
	}
	if len(cfg.Contracts) == 0 {
		cfg.Contracts = DefaultContracts
	}

	contractFilters := make(map[string]bool)
	for _, c := range cfg.Contracts {
		contractFilters[strings.ToUpper(c)] = true
	}

	baseCfg := provider.ProviderConfig{
		Name:           "cme_cot",
		Category:       signal.CategoryMacro,
		PollInterval:   cfg.PollInterval,
		RequestTimeout: 60 * time.Second,
		MaxRetries:     3,
		RetryBackoff:   30 * time.Second,
	}

	return &Provider{
		BaseProvider:    provider.NewBaseProvider(baseCfg, logger.Named("cme_cot")),
		config:          cfg,
		httpClient:      &http.Client{Timeout: 60 * time.Second},
		previousData:    make(map[string]*COTData),
		contractFilters: contractFilters,
	}
}

func (p *Provider) Start(ctx context.Context) error {
	if !p.config.Enabled {
		p.Logger().Info("CME COT provider is disabled")
		return nil
	}

	p.MarkStarted()

	p.wg.Add(1)
	go p.pollLoop(ctx)

	p.Logger().Info("CME COT provider started",
		zap.Duration("poll_interval", p.config.PollInterval),
		zap.Int("contracts_tracked", len(p.config.Contracts)),
	)

	return nil
}

func (p *Provider) Stop() error {
	err := p.BaseProvider.Stop()
	p.wg.Wait()
	p.Logger().Info("CME COT provider stopped")
	return err
}

func (p *Provider) pollLoop(ctx context.Context) {
	defer p.wg.Done()

	ticker := time.NewTicker(p.config.PollInterval)
	defer ticker.Stop()

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

func (p *Provider) poll(ctx context.Context) error {
	year := time.Now().Year()

	urls := []string{
		fmt.Sprintf("%s/deacot%d.zip", p.config.BaseURL, year),
		fmt.Sprintf("%s/fut_fin_txt_%d.zip", p.config.BaseURL, year),
		fmt.Sprintf("%s/deacot%d.zip", p.config.BaseURL, year-1),
		fmt.Sprintf("%s/fut_fin_txt_%d.zip", p.config.BaseURL, year-1),
	}

	var data []*COTData
	var err error
	for _, url := range urls {
		data, err = p.fetchAndParseCOT(ctx, url)
		if err == nil && len(data) > 0 {
			p.Logger().Debug("Successfully fetched COT data", zap.String("url", url))
			break
		}
	}

	if err != nil || len(data) == 0 {
		return fmt.Errorf("fetch COT data from all sources: %w", err)
	}

	for _, cot := range data {
		p.mu.Lock()
		prev := p.previousData[cot.ContractName]
		isNew := prev == nil || cot.ReportDate.After(prev.ReportDate)
		if isNew {
			p.previousData[cot.ContractName] = cot
		}
		p.mu.Unlock()

		if isNew {
			if err := p.emitCOTSignal(ctx, cot, prev); err != nil {
				p.Logger().Warn("Failed to emit signal",
					zap.Error(err),
					zap.String("contract", cot.ContractName),
				)
			}
		}
	}

	return nil
}

func (p *Provider) fetchAndParseCOT(ctx context.Context, url string) ([]*COTData, error) {
	p.Logger().Debug("Fetching COT data", zap.String("url", url))

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (compatible; ACC-L1-Ingestion/1.0)")
	req.Header.Set("Accept", "application/zip, application/octet-stream, */*")

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	zipReader, err := zip.NewReader(bytes.NewReader(body), int64(len(body)))
	if err != nil {
		return nil, fmt.Errorf("open zip: %w", err)
	}

	for _, f := range zipReader.File {
		if strings.HasSuffix(strings.ToLower(f.Name), ".csv") ||
			strings.HasSuffix(strings.ToLower(f.Name), ".txt") {
			rc, err := f.Open()
			if err != nil {
				continue
			}
			defer rc.Close()
			return p.parseCSV(rc)
		}
	}

	return nil, fmt.Errorf("no CSV/TXT file found in zip archive")
}

func (p *Provider) parseCSV(reader io.Reader) ([]*COTData, error) {
	csvReader := csv.NewReader(reader)
	csvReader.LazyQuotes = true
	csvReader.TrimLeadingSpace = true

	headers, err := csvReader.Read()
	if err != nil {
		return nil, fmt.Errorf("read headers: %w", err)
	}

	colIndex := make(map[string]int)
	for i, h := range headers {
		colIndex[strings.ToUpper(strings.TrimSpace(h))] = i
	}

	var results []*COTData

	for {
		record, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			continue
		}

		cot, err := p.parseRecord(record, colIndex)
		if err != nil {
			continue
		}

		if !p.contractFilters[strings.ToUpper(cot.ContractName)] {
			continue
		}

		results = append(results, cot)
	}

	return results, nil
}

func (p *Provider) parseRecord(record []string, cols map[string]int) (*COTData, error) {
	get := func(name string) string {
		if idx, ok := cols[name]; ok && idx < len(record) {
			return strings.TrimSpace(record[idx])
		}
		return ""
	}

	getInt := func(name string) int64 {
		s := get(name)
		if s == "" {
			return 0
		}
		s = strings.ReplaceAll(s, ",", "")
		v, _ := strconv.ParseInt(s, 10, 64)
		return v
	}

	contractName := get("MARKET_AND_EXCHANGE_NAMES")
	if contractName == "" {
		contractName = get("MARKET AND EXCHANGE NAMES")
	}
	if contractName == "" {
		return nil, fmt.Errorf("missing contract name")
	}

	dateStr := get("REPORT_DATE_AS_YYYY-MM-DD")
	if dateStr == "" {
		dateStr = get("AS_OF_DATE_IN_FORM_YYMMDD")
	}
	reportDate, err := parseDate(dateStr)
	if err != nil {
		return nil, fmt.Errorf("parse date: %w", err)
	}

	nonCommLong := getInt("NONCOMM_POSITIONS_LONG_ALL")
	if nonCommLong == 0 {
		nonCommLong = getInt("NON-COMMERCIAL LONG")
	}
	nonCommShort := getInt("NONCOMM_POSITIONS_SHORT_ALL")
	if nonCommShort == 0 {
		nonCommShort = getInt("NON-COMMERCIAL SHORT")
	}

	commLong := getInt("COMM_POSITIONS_LONG_ALL")
	if commLong == 0 {
		commLong = getInt("COMMERCIAL LONG")
	}
	commShort := getInt("COMM_POSITIONS_SHORT_ALL")
	if commShort == 0 {
		commShort = getInt("COMMERCIAL SHORT")
	}

	openInterest := getInt("OPEN_INTEREST_ALL")
	if openInterest == 0 {
		openInterest = getInt("OPEN INTEREST")
	}

	cot := &COTData{
		ContractName:       contractName,
		ReportDate:         reportDate,
		OpenInterest:       openInterest,
		NonCommLong:        nonCommLong,
		NonCommShort:       nonCommShort,
		NonCommSpreads:     getInt("NONCOMM_POSITIONS_SPREAD_ALL"),
		CommLong:           commLong,
		CommShort:          commShort,
		NonReportableLong:  getInt("NONREPT_POSITIONS_LONG_ALL"),
		NonReportableShort: getInt("NONREPT_POSITIONS_SHORT_ALL"),
		NetNonComm:         nonCommLong - nonCommShort,
		NetComm:            commLong - commShort,
	}

	if openInterest > 0 {
		cot.PercentNonCommLong = float64(nonCommLong) / float64(openInterest) * 100
		cot.PercentCommLong = float64(commLong) / float64(openInterest) * 100
	}

	return cot, nil
}

func parseDate(s string) (time.Time, error) {
	formats := []string{
		"2006-01-02",
		"060102",
		"01/02/2006",
		"1/2/2006",
	}
	for _, f := range formats {
		if t, err := time.Parse(f, s); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("unknown date format: %s", s)
}

func (p *Provider) emitCOTSignal(ctx context.Context, cot *COTData, prev *COTData) error {
	action := determineCOTAction(cot, prev)
	sentiment := calculateCOTSentiment(cot, prev)
	urgency := calculateCOTUrgency(cot, prev)

	subject := simplifyContractName(cot.ContractName)
	object := formatPositioning(cot)

	tags := generateCOTTags(cot, prev)

	builder := signal.NewBuilder(signal.SourceCMECOT, signal.CategoryMacro).
		WithTimestamp(cot.ReportDate).
		WithSubject(subject).
		WithAction(action).
		WithObject(object).
		WithConfidence(0.99).
		WithSentiment(sentiment).
		WithUrgency(urgency).
		WithTags(tags...).
		WithProviderMeta(map[string]any{
			"source_id":   fmt.Sprintf("cot_%s_%s", simplifyContractName(cot.ContractName), cot.ReportDate.Format("20060102")),
			"report_date": cot.ReportDate.Format("2006-01-02"),
		}).
		WithRawData(map[string]interface{}{
			"contract_name":        cot.ContractName,
			"report_date":          cot.ReportDate.Format("2006-01-02"),
			"open_interest":        cot.OpenInterest,
			"non_comm_long":        cot.NonCommLong,
			"non_comm_short":       cot.NonCommShort,
			"non_comm_spreads":     cot.NonCommSpreads,
			"comm_long":            cot.CommLong,
			"comm_short":           cot.CommShort,
			"non_reportable_long":  cot.NonReportableLong,
			"non_reportable_short": cot.NonReportableShort,
			"net_non_comm":         cot.NetNonComm,
			"net_comm":             cot.NetComm,
			"pct_non_comm_long":    cot.PercentNonCommLong,
			"pct_comm_long":        cot.PercentCommLong,
		})

	sig, err := builder.Build()
	if err != nil {
		return fmt.Errorf("build signal: %w", err)
	}

	if err := p.Emit(ctx, sig); err != nil {
		return fmt.Errorf("emit signal: %w", err)
	}

	p.Logger().Info("COT report processed",
		zap.String("contract", subject),
		zap.Time("report_date", cot.ReportDate),
		zap.Int64("net_non_comm", cot.NetNonComm),
		zap.Int64("net_comm", cot.NetComm),
	)

	return nil
}

func determineCOTAction(cot *COTData, prev *COTData) string {
	if prev == nil {
		return "cot_report"
	}

	netChange := cot.NetNonComm - prev.NetNonComm
	if netChange > 0 {
		return "spec_position_increase"
	} else if netChange < 0 {
		return "spec_position_decrease"
	}
	return "cot_report"
}

func calculateCOTSentiment(cot *COTData, prev *COTData) float64 {
	if cot.OpenInterest == 0 {
		return 0
	}

	netRatio := float64(cot.NetNonComm) / float64(cot.OpenInterest)
	sentiment := netRatio * 2

	if sentiment > 1.0 {
		sentiment = 1.0
	} else if sentiment < -1.0 {
		sentiment = -1.0
	}

	return sentiment
}

func calculateCOTUrgency(cot *COTData, prev *COTData) signal.UrgencyLevel {
	if prev == nil {
		return signal.UrgencyLow
	}

	if prev.OpenInterest == 0 {
		return signal.UrgencyLow
	}

	netChange := cot.NetNonComm - prev.NetNonComm
	changePercent := float64(netChange) / float64(prev.OpenInterest) * 100

	absChange := changePercent
	if absChange < 0 {
		absChange = -absChange
	}

	switch {
	case absChange >= 10:
		return signal.UrgencyHigh
	case absChange >= 5:
		return signal.UrgencyMedium
	default:
		return signal.UrgencyLow
	}
}

func simplifyContractName(name string) string {
	name = strings.ToUpper(name)

	simplifications := map[string]string{
		"GOLD - COMMODITY EXCHANGE INC.":                "Gold",
		"SILVER - COMMODITY EXCHANGE INC.":              "Silver",
		"WTI CRUDE OIL - NEW YORK MERCANTILE EXCHANGE":  "WTI Crude",
		"NATURAL GAS - NEW YORK MERCANTILE EXCHANGE":    "Natural Gas",
		"E-MINI S&P 500 - CHICAGO MERCANTILE EXCHANGE":  "S&P 500",
		"NASDAQ MINI - CHICAGO MERCANTILE EXCHANGE":     "Nasdaq",
		"U.S. DOLLAR INDEX - ICE FUTURES U.S.":          "US Dollar Index",
		"EURO FX - CHICAGO MERCANTILE EXCHANGE":         "EUR/USD",
		"JAPANESE YEN - CHICAGO MERCANTILE EXCHANGE":    "USD/JPY",
		"BITCOIN - CHICAGO MERCANTILE EXCHANGE":         "Bitcoin CME",
		"COPPER - COMMODITY EXCHANGE INC.":              "Copper",
		"PLATINUM - NEW YORK MERCANTILE EXCHANGE":       "Platinum",
		"PALLADIUM - NEW YORK MERCANTILE EXCHANGE":      "Palladium",
		"CORN - CHICAGO BOARD OF TRADE":                 "Corn",
		"WHEAT - CHICAGO BOARD OF TRADE":                "Wheat",
		"SOYBEANS - CHICAGO BOARD OF TRADE":             "Soybeans",
		"10-YEAR U.S. TREASURY NOTES - CHICAGO BOARD":   "10Y Treasury",
		"2-YEAR U.S. TREASURY NOTES - CHICAGO BOARD":    "2Y Treasury",
		"30-DAY FEDERAL FUNDS - CHICAGO BOARD OF TRADE": "Fed Funds",
	}

	for full, simple := range simplifications {
		if strings.Contains(name, full) {
			return simple
		}
	}

	parts := strings.Split(name, " - ")
	if len(parts) > 0 {
		return strings.Title(strings.ToLower(parts[0]))
	}

	return name
}

func formatPositioning(cot *COTData) string {
	position := "neutral"
	if cot.NetNonComm > 0 {
		position = "net long"
	} else if cot.NetNonComm < 0 {
		position = "net short"
	}

	return fmt.Sprintf("Speculators %s %d contracts", position, abs(cot.NetNonComm))
}

func abs(n int64) int64 {
	if n < 0 {
		return -n
	}
	return n
}

func generateCOTTags(cot *COTData, prev *COTData) []string {
	tags := []string{"cot", "futures", "positioning"}

	name := strings.ToLower(simplifyContractName(cot.ContractName))
	tags = append(tags, strings.ReplaceAll(name, " ", "-"))

	switch {
	case strings.Contains(name, "gold"), strings.Contains(name, "silver"),
		strings.Contains(name, "copper"), strings.Contains(name, "platinum"):
		tags = append(tags, "metals")
	case strings.Contains(name, "crude"), strings.Contains(name, "gas"):
		tags = append(tags, "energy")
	case strings.Contains(name, "corn"), strings.Contains(name, "wheat"),
		strings.Contains(name, "soy"):
		tags = append(tags, "agriculture")
	case strings.Contains(name, "s&p"), strings.Contains(name, "nasdaq"):
		tags = append(tags, "equity-index")
	case strings.Contains(name, "eur"), strings.Contains(name, "jpy"),
		strings.Contains(name, "dollar"):
		tags = append(tags, "forex")
	case strings.Contains(name, "treasury"), strings.Contains(name, "fed"):
		tags = append(tags, "rates")
	case strings.Contains(name, "bitcoin"):
		tags = append(tags, "crypto")
	}

	if cot.NetNonComm > 0 {
		tags = append(tags, "spec-long")
	} else if cot.NetNonComm < 0 {
		tags = append(tags, "spec-short")
	}

	if prev != nil {
		change := cot.NetNonComm - prev.NetNonComm
		if change > 0 {
			tags = append(tags, "increasing-longs")
		} else if change < 0 {
			tags = append(tags, "increasing-shorts")
		}
	}

	return tags
}
