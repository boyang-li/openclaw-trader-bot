// Package gdelt provides a provider for GDELT (Global Database of Events, Language, and Tone).
// It polls the GDELT 2.0 GKG (Global Knowledge Graph) files for geopolitical events.
package gdelt

import (
	"archive/zip"
	"bufio"
	"bytes"
	"context"
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
	// DefaultBaseURL is the GDELT 2.0 data endpoint
	DefaultBaseURL = "http://data.gdeltproject.org/gdeltv2"

	// DefaultPollInterval is how often to check for new files
	DefaultPollInterval = 15 * time.Minute

	// GKG files are published every 15 minutes
	gkgFileInterval = 15 * time.Minute
)

// Config holds GDELT provider configuration.
type Config struct {
	BaseURL         string
	PollInterval    time.Duration
	BatchSize       int
	Enabled         bool
	FilterCountries []string // Countries to filter for (empty = all)
	FilterThemes    []string // Themes to filter for (empty = all)
	MinTone         float64  // Minimum tone threshold (negative = negative news)
}

// DefaultConfig returns sensible defaults for GDELT provider.
func DefaultConfig() Config {
	return Config{
		BaseURL:      DefaultBaseURL,
		PollInterval: DefaultPollInterval,
		BatchSize:    250,
		Enabled:      true,
		MinTone:      -100, // Accept all tones by default
	}
}

// Provider implements the provider.Provider interface for GDELT data.
type Provider struct {
	*provider.BaseProvider

	config     Config
	httpClient *http.Client

	mu            sync.RWMutex
	lastProcessed time.Time

	wg sync.WaitGroup
}

// New creates a new GDELT provider.
func New(cfg Config, logger *zap.Logger) *Provider {
	if logger == nil {
		logger = zap.NewNop()
	}

	baseCfg := provider.ProviderConfig{
		Name:           "gdelt",
		Category:       signal.CategoryGeopolitical,
		PollInterval:   cfg.PollInterval,
		RequestTimeout: 60 * time.Second,
		MaxRetries:     3,
		RetryBackoff:   time.Second,
	}

	return &Provider{
		BaseProvider: provider.NewBaseProvider(baseCfg, logger.Named("gdelt")),
		config:       cfg,
		httpClient: &http.Client{
			Timeout: 60 * time.Second,
		},
		lastProcessed: time.Time{},
	}
}

// Start begins polling GDELT for new data.
func (p *Provider) Start(ctx context.Context) error {
	if !p.config.Enabled {
		p.Logger().Info("GDELT provider is disabled")
		return nil
	}

	p.MarkStarted()

	p.wg.Add(1)
	go p.pollLoop(ctx)

	p.Logger().Info("GDELT provider started",
		zap.Duration("poll_interval", p.config.PollInterval),
		zap.Int("batch_size", p.config.BatchSize),
	)

	return nil
}

// Stop gracefully stops the provider.
func (p *Provider) Stop() error {
	err := p.BaseProvider.Stop()
	p.wg.Wait()
	p.Logger().Info("GDELT provider stopped")
	return err
}

// pollLoop continuously polls for new GDELT data.
func (p *Provider) pollLoop(ctx context.Context) {
	defer p.wg.Done()

	// Initial poll
	p.poll(ctx)

	ticker := time.NewTicker(p.config.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-p.StopChannel():
			return
		case <-ticker.C:
			p.poll(ctx)
		}
	}
}

// poll fetches and processes the latest GDELT GKG file.
func (p *Provider) poll(ctx context.Context) {
	p.Logger().Debug("Polling GDELT for new data")

	// Calculate the timestamp for the latest available file
	// GDELT files are published ~15 minutes after the timestamp
	fileTime := p.getLatestFileTime()

	// Skip if we've already processed this file
	p.mu.RLock()
	lastProcessed := p.lastProcessed
	p.mu.RUnlock()

	if !fileTime.After(lastProcessed) {
		p.Logger().Debug("No new GDELT file available",
			zap.Time("last_processed", lastProcessed),
			zap.Time("latest_file", fileTime),
		)
		return
	}

	// Fetch and process the GKG file
	signals, err := p.fetchAndParseGKG(ctx, fileTime)
	if err != nil {
		p.Logger().Error("Failed to fetch GDELT data", zap.Error(err))
		p.RecordError(err)
		return
	}

	// Emit signals to handlers
	for _, sig := range signals {
		if err := p.Emit(ctx, sig); err != nil {
			p.Logger().Warn("Failed to emit signal", zap.Error(err))
		}
	}

	// Update last processed time
	p.mu.Lock()
	p.lastProcessed = fileTime
	p.mu.Unlock()

	p.Logger().Info("Processed GDELT data",
		zap.Int("signals", len(signals)),
		zap.Time("file_time", fileTime),
	)
}

// getLatestFileTime calculates the timestamp for the latest available GKG file.
func (p *Provider) getLatestFileTime() time.Time {
	now := time.Now().UTC()

	// Round down to the nearest 15-minute interval
	minutes := now.Minute()
	roundedMinutes := (minutes / 15) * 15

	// Subtract 15 minutes to account for publication delay
	return time.Date(
		now.Year(), now.Month(), now.Day(),
		now.Hour(), roundedMinutes, 0, 0, time.UTC,
	).Add(-gkgFileInterval)
}

// buildGKGURL constructs the URL for a GKG file at the given time.
func (p *Provider) buildGKGURL(t time.Time) string {
	// Format: YYYYMMDDHHMMSS.gkg.csv.zip
	filename := t.Format("20060102150405") + ".gkg.csv.zip"
	return fmt.Sprintf("%s/%s", p.config.BaseURL, filename)
}

// fetchAndParseGKG downloads and parses a GDELT GKG file.
func (p *Provider) fetchAndParseGKG(ctx context.Context, fileTime time.Time) ([]signal.Signal, error) {
	url := p.buildGKGURL(fileTime)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch GKG file: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	zipReader, err := zip.NewReader(bytes.NewReader(body), int64(len(body)))
	if err != nil {
		return nil, fmt.Errorf("open zip: %w", err)
	}

	if len(zipReader.File) == 0 {
		return nil, fmt.Errorf("empty zip archive")
	}

	csvFile, err := zipReader.File[0].Open()
	if err != nil {
		return nil, fmt.Errorf("open csv in zip: %w", err)
	}
	defer csvFile.Close()

	return p.parseGKG(csvFile, fileTime)
}

// parseGKG parses GKG CSV data into signals.
func (p *Provider) parseGKG(r io.Reader, fileTime time.Time) ([]signal.Signal, error) {
	scanner := bufio.NewScanner(r)
	// Increase buffer size for large lines
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)

	var signals []signal.Signal
	lineNum := 0

	for scanner.Scan() {
		lineNum++
		if lineNum > p.config.BatchSize {
			break // Limit batch size
		}

		line := scanner.Text()
		sig, ok := p.parseGKGLine(line, fileTime)
		if ok {
			signals = append(signals, sig)
		}
	}

	if err := scanner.Err(); err != nil {
		return signals, fmt.Errorf("scan error: %w", err)
	}

	return signals, nil
}

// parseGKGLine parses a single GKG record into a signal.
// GKG 2.0 format: https://blog.gdeltproject.org/gdelt-2-0-our-global-world-in-realtime/
func (p *Provider) parseGKGLine(line string, fileTime time.Time) (signal.Signal, bool) {
	fields := strings.Split(line, "\t")
	if len(fields) < 27 {
		return signal.Signal{}, false
	}

	// Field indices (0-based):
	// 0: GKGRECORDID
	// 1: DATE (YYYYMMDDHHMMSS)
	// 3: SourceCollectionIdentifier
	// 4: SourceCommonName
	// 7: Themes
	// 9: Locations
	// 11: Persons
	// 13: Organizations
	// 15: Tone
	// 17: GCAM (Global Content Analysis Measures)

	// Parse tone (average tone of the article)
	tone := p.parseTone(fields[15])
	if tone < p.config.MinTone {
		return signal.Signal{}, false
	}

	// Extract themes
	themes := p.parseThemes(fields[7])
	if len(p.config.FilterThemes) > 0 && !p.hasMatchingTheme(themes) {
		return signal.Signal{}, false
	}

	// Extract locations
	locations := p.parseLocations(fields[9])
	if len(p.config.FilterCountries) > 0 && !p.hasMatchingCountry(locations) {
		return signal.Signal{}, false
	}

	// Extract organizations and persons for subject/object
	orgs := p.parseEntities(fields[13])
	persons := p.parseEntities(fields[11])

	// Determine subject and action
	subject := p.determineSubject(locations, orgs, persons)
	action := p.determineAction(themes)
	object := p.determineObject(orgs, persons, subject)

	if subject == "" || action == "" {
		return signal.Signal{}, false
	}

	// Calculate confidence based on source and data quality
	confidence := p.calculateConfidence(fields)

	// Convert tone to sentiment (-10 to +10 -> -1 to +1)
	sentiment := tone / 10.0
	if sentiment < -1 {
		sentiment = -1
	} else if sentiment > 1 {
		sentiment = 1
	}

	// Determine urgency based on tone and themes
	urgency := p.determineUrgency(tone, themes)

	// Build the signal
	builder := signal.NewBuilder(signal.SourceGDELT, signal.CategoryGeopolitical).
		WithTimestamp(fileTime).
		WithSubject(subject).
		WithAction(action).
		WithObject(object).
		WithConfidence(confidence).
		WithSentiment(sentiment).
		WithUrgency(urgency).
		WithTags(themes...).
		WithRawData(map[string]interface{}{
			"gkg_record_id": fields[0],
			"source":        safeGet(fields, 4),
			"tone":          tone,
			"locations":     locations,
			"organizations": orgs,
			"persons":       persons,
		})

	sig, err := builder.Build()
	if err != nil {
		return signal.Signal{}, false
	}

	return sig, true
}

// parseTone extracts the average tone from the tone field.
func (p *Provider) parseTone(toneField string) float64 {
	parts := strings.Split(toneField, ",")
	if len(parts) == 0 {
		return 0
	}
	tone, _ := strconv.ParseFloat(parts[0], 64)
	return tone
}

// parseThemes extracts theme codes from the themes field.
func (p *Provider) parseThemes(themesField string) []string {
	if themesField == "" {
		return nil
	}

	parts := strings.Split(themesField, ";")
	themes := make([]string, 0, len(parts))
	seen := make(map[string]bool)

	for _, part := range parts {
		// Theme format: THEME_CODE,CharOffset
		theme := strings.Split(part, ",")[0]
		theme = strings.ToLower(theme)

		if theme != "" && !seen[theme] {
			seen[theme] = true
			themes = append(themes, theme)
		}
	}

	return themes
}

// parseLocations extracts location information.
func (p *Provider) parseLocations(locField string) []string {
	if locField == "" {
		return nil
	}

	parts := strings.Split(locField, ";")
	locations := make([]string, 0, len(parts))
	seen := make(map[string]bool)

	for _, part := range parts {
		// Location format: Type#FullName#CountryCode#ADM1Code#ADM2Code#Lat#Long#FeatureID
		fields := strings.Split(part, "#")
		if len(fields) >= 3 {
			country := fields[2]
			if country != "" && !seen[country] {
				seen[country] = true
				locations = append(locations, country)
			}
		}
	}

	return locations
}

// parseEntities extracts entity names (persons or organizations).
func (p *Provider) parseEntities(field string) []string {
	if field == "" {
		return nil
	}

	parts := strings.Split(field, ";")
	entities := make([]string, 0, len(parts))
	seen := make(map[string]bool)

	for _, part := range parts {
		// Entity format: Name,CharOffset
		name := strings.Split(part, ",")[0]
		name = strings.TrimSpace(name)

		if name != "" && !seen[name] {
			seen[name] = true
			entities = append(entities, name)
		}
	}

	return entities
}

// hasMatchingTheme checks if any theme matches the filter.
func (p *Provider) hasMatchingTheme(themes []string) bool {
	for _, theme := range themes {
		for _, filter := range p.config.FilterThemes {
			if strings.Contains(theme, strings.ToLower(filter)) {
				return true
			}
		}
	}
	return false
}

// hasMatchingCountry checks if any location matches the filter.
func (p *Provider) hasMatchingCountry(locations []string) bool {
	for _, loc := range locations {
		for _, filter := range p.config.FilterCountries {
			if strings.EqualFold(loc, filter) {
				return true
			}
		}
	}
	return false
}

// determineSubject picks the primary subject from available data.
func (p *Provider) determineSubject(locations, orgs, persons []string) string {
	// Prefer country, then organization, then person
	if len(locations) > 0 {
		return locations[0]
	}
	if len(orgs) > 0 {
		return orgs[0]
	}
	if len(persons) > 0 {
		return persons[0]
	}
	return ""
}

// determineAction maps themes to action types.
func (p *Provider) determineAction(themes []string) string {
	// Map common GDELT themes to actions
	themeActions := map[string]string{
		"military":   "military_activity",
		"protest":    "civil_unrest",
		"terror":     "security_incident",
		"econ_":      "economic_event",
		"trade":      "trade_activity",
		"election":   "political_event",
		"diplomacy":  "diplomatic_activity",
		"sanctions":  "sanctions",
		"conflict":   "conflict",
		"cyber":      "cyber_activity",
		"health":     "health_event",
		"disaster":   "natural_disaster",
		"migration":  "migration",
		"leadership": "leadership_change",
	}

	for _, theme := range themes {
		for prefix, action := range themeActions {
			if strings.Contains(theme, prefix) {
				return action
			}
		}
	}

	return "geopolitical_event" // Default action
}

// determineObject picks a secondary entity for the object field.
func (p *Provider) determineObject(orgs, persons []string, subject string) string {
	// Pick an entity that's different from the subject
	for _, org := range orgs {
		if org != subject {
			return org
		}
	}
	for _, person := range persons {
		if person != subject {
			return person
		}
	}
	return ""
}

// calculateConfidence estimates confidence based on source quality.
func (p *Provider) calculateConfidence(fields []string) float64 {
	// Base confidence for GDELT data
	confidence := 0.6

	// Boost for known reliable sources
	source := strings.ToLower(safeGet(fields, 4))
	reliableSources := []string{"reuters", "ap", "afp", "bbc", "nytimes", "washingtonpost"}
	for _, rs := range reliableSources {
		if strings.Contains(source, rs) {
			confidence = 0.8
			break
		}
	}

	return confidence
}

// determineUrgency calculates urgency based on tone and themes.
func (p *Provider) determineUrgency(tone float64, themes []string) signal.UrgencyLevel {
	// Very negative tone indicates urgent news
	if tone < -5 {
		return signal.UrgencyHigh
	}

	// Check for urgent themes
	urgentThemes := []string{"terror", "military", "conflict", "disaster", "crisis"}
	for _, theme := range themes {
		for _, urgent := range urgentThemes {
			if strings.Contains(theme, urgent) {
				return signal.UrgencyMedium
			}
		}
	}

	return signal.UrgencyLow
}

// safeGet safely gets a field from a slice.
func safeGet(fields []string, idx int) string {
	if idx < len(fields) {
		return fields[idx]
	}
	return ""
}
