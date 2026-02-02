package signal

import (
	"testing"
	"time"
)

func TestSignalBuilder_Success(t *testing.T) {
	sig, err := NewBuilder(SourceGDELT, CategoryGeopolitical).
		WithSubject("China").
		WithAction("sanctions").
		WithObject("Russia").
		WithConfidence(0.85).
		WithSentiment(-0.5).
		WithUrgency(UrgencyHigh).
		WithTags("trade", "asia").
		Build()

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if sig.Subject != "China" {
		t.Errorf("expected subject 'China', got %q", sig.Subject)
	}
	if sig.Action != "sanctions" {
		t.Errorf("expected action 'sanctions', got %q", sig.Action)
	}
	if sig.Confidence != 0.85 {
		t.Errorf("expected confidence 0.85, got %v", sig.Confidence)
	}
}

func TestSignalBuilder_ValidationErrors(t *testing.T) {
	tests := []struct {
		name    string
		builder func() *Builder
		wantErr error
	}{
		{
			name: "empty subject",
			builder: func() *Builder {
				return NewBuilder(SourceGDELT, CategoryGeopolitical).
					WithAction("test")
			},
			wantErr: ErrEmptySubject,
		},
		{
			name: "empty action",
			builder: func() *Builder {
				return NewBuilder(SourceGDELT, CategoryGeopolitical).
					WithSubject("test")
			},
			wantErr: ErrEmptyAction,
		},
		{
			name: "invalid confidence high",
			builder: func() *Builder {
				return NewBuilder(SourceGDELT, CategoryGeopolitical).
					WithSubject("test").
					WithAction("test").
					WithConfidence(1.5)
			},
			wantErr: ErrInvalidConfidence,
		},
		{
			name: "invalid confidence low",
			builder: func() *Builder {
				return NewBuilder(SourceGDELT, CategoryGeopolitical).
					WithSubject("test").
					WithAction("test").
					WithConfidence(-0.1)
			},
			wantErr: ErrInvalidConfidence,
		},
		{
			name: "invalid sentiment high",
			builder: func() *Builder {
				return NewBuilder(SourceGDELT, CategoryGeopolitical).
					WithSubject("test").
					WithAction("test").
					WithSentiment(1.5)
			},
			wantErr: ErrInvalidSentiment,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := tt.builder().Build()
			if err != tt.wantErr {
				t.Errorf("expected error %v, got %v", tt.wantErr, err)
			}
		})
	}
}

func TestSignal_ToJSON_FromJSON(t *testing.T) {
	original, _ := NewBuilder(SourceFRED, CategoryMacro).
		WithSubject("FEDFUNDS").
		WithAction("rate_change").
		WithConfidence(0.95).
		WithTimestamp(time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC)).
		Build()

	data, err := original.ToJSON()
	if err != nil {
		t.Fatalf("ToJSON failed: %v", err)
	}

	restored, err := FromJSON(data)
	if err != nil {
		t.Fatalf("FromJSON failed: %v", err)
	}

	if restored.Subject != original.Subject {
		t.Errorf("subject mismatch: got %q, want %q", restored.Subject, original.Subject)
	}
	if restored.Source != original.Source {
		t.Errorf("source mismatch: got %q, want %q", restored.Source, original.Source)
	}
}

func TestSignal_KafkaKey(t *testing.T) {
	sig, _ := NewBuilder(SourceBinance, CategoryCrypto).
		WithSubject("BTCUSDT").
		WithAction("large_trade").
		Build()

	key := sig.KafkaKey()
	expected := "binance:BTCUSDT"
	if key != expected {
		t.Errorf("expected key %q, got %q", expected, key)
	}
}

func TestSource_IsValid(t *testing.T) {
	tests := []struct {
		source Source
		valid  bool
	}{
		{SourceGDELT, true},
		{SourceFRED, true},
		{SourceBinance, true},
		{Source("invalid"), false},
		{Source(""), false},
	}

	for _, tt := range tests {
		t.Run(string(tt.source), func(t *testing.T) {
			if got := tt.source.IsValid(); got != tt.valid {
				t.Errorf("Source(%q).IsValid() = %v, want %v", tt.source, got, tt.valid)
			}
		})
	}
}

func TestCategory_IsValid(t *testing.T) {
	tests := []struct {
		cat   Category
		valid bool
	}{
		{CategoryGeopolitical, true},
		{CategoryMacro, true},
		{CategoryCrypto, true},
		{Category("invalid"), false},
	}

	for _, tt := range tests {
		t.Run(string(tt.cat), func(t *testing.T) {
			if got := tt.cat.IsValid(); got != tt.valid {
				t.Errorf("Category(%q).IsValid() = %v, want %v", tt.cat, got, tt.valid)
			}
		})
	}
}

func TestParseSource(t *testing.T) {
	src, err := ParseSource("gdelt")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if src != SourceGDELT {
		t.Errorf("expected SourceGDELT, got %v", src)
	}

	_, err = ParseSource("invalid")
	if err == nil {
		t.Error("expected error for invalid source")
	}
}

func TestParseCategory(t *testing.T) {
	cat, err := ParseCategory("macro")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cat != CategoryMacro {
		t.Errorf("expected CategoryMacro, got %v", cat)
	}

	_, err = ParseCategory("invalid")
	if err == nil {
		t.Error("expected error for invalid category")
	}
}
