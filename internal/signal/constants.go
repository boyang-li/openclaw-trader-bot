package signal

import "fmt"

const SchemaVersion = "1.0.0"

type Source string

const (
	SourceGDELT       Source = "gdelt"
	SourceFRED        Source = "fred"
	SourceBinance     Source = "binance"
	SourceTelegram    Source = "telegram"
	SourceWhaleAlert  Source = "whale_alert"
	SourceTradingEcon Source = "trading_economics"
	SourceCMECOT      Source = "cme_cot"
)

var validSources = map[Source]bool{
	SourceGDELT:       true,
	SourceFRED:        true,
	SourceBinance:     true,
	SourceTelegram:    true,
	SourceWhaleAlert:  true,
	SourceTradingEcon: true,
	SourceCMECOT:      true,
}

func (s Source) IsValid() bool {
	return validSources[s]
}

func (s Source) String() string {
	return string(s)
}

func ParseSource(s string) (Source, error) {
	src := Source(s)
	if !src.IsValid() {
		return "", fmt.Errorf("signal: invalid source %q", s)
	}
	return src, nil
}

type Category string

const (
	CategoryGeopolitical Category = "geopolitical"
	CategoryMacro        Category = "macro"
	CategoryCrypto       Category = "crypto"
)

var validCategories = map[Category]bool{
	CategoryGeopolitical: true,
	CategoryMacro:        true,
	CategoryCrypto:       true,
}

func (c Category) IsValid() bool {
	return validCategories[c]
}

func (c Category) String() string {
	return string(c)
}

func ParseCategory(s string) (Category, error) {
	cat := Category(s)
	if !cat.IsValid() {
		return "", fmt.Errorf("signal: invalid category %q", s)
	}
	return cat, nil
}

type UrgencyLevel string

const (
	UrgencyLow      UrgencyLevel = "low"
	UrgencyMedium   UrgencyLevel = "medium"
	UrgencyHigh     UrgencyLevel = "high"
	UrgencyCritical UrgencyLevel = "critical"
)

var validUrgencyLevels = map[UrgencyLevel]bool{
	UrgencyLow:      true,
	UrgencyMedium:   true,
	UrgencyHigh:     true,
	UrgencyCritical: true,
}

func (u UrgencyLevel) IsValid() bool {
	return validUrgencyLevels[u]
}

func (u UrgencyLevel) String() string {
	return string(u)
}

func ParseUrgencyLevel(s string) (UrgencyLevel, error) {
	lvl := UrgencyLevel(s)
	if !lvl.IsValid() {
		return "", fmt.Errorf("signal: invalid urgency level %q", s)
	}
	return lvl, nil
}
