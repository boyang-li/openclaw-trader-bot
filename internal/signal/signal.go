package signal

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

type Signal struct {
	ID        uuid.UUID `json:"id"`
	Timestamp time.Time `json:"timestamp"`

	Source   Source   `json:"source"`
	Category Category `json:"category"`

	Subject string `json:"subject"`
	Action  string `json:"action"`
	Object  string `json:"object,omitempty"`

	Confidence float64      `json:"confidence"`
	Sentiment  float64      `json:"sentiment"`
	Urgency    UrgencyLevel `json:"urgency"`
	Tags       []string     `json:"tags,omitempty"`

	RawData  json.RawMessage `json:"raw_data,omitempty"`
	Metadata Metadata        `json:"metadata"`
}

type Metadata struct {
	IngestedAt    time.Time      `json:"ingested_at"`
	ProcessedAt   *time.Time     `json:"processed_at,omitempty"`
	ProviderMeta  map[string]any `json:"provider_meta,omitempty"`
	SchemaVersion string         `json:"schema_version"`
	TraceID       string         `json:"trace_id,omitempty"`
}

func (s *Signal) ToJSON() ([]byte, error) {
	return json.Marshal(s)
}

func FromJSON(data []byte) (*Signal, error) {
	var sig Signal
	if err := json.Unmarshal(data, &sig); err != nil {
		return nil, fmt.Errorf("signal: failed to unmarshal: %w", err)
	}
	return &sig, nil
}

func (s *Signal) KafkaKey() string {
	return fmt.Sprintf("%s:%s", s.Source, s.Subject)
}

func (s *Signal) Validate() error {
	if s.Subject == "" {
		return ErrEmptySubject
	}
	if s.Action == "" {
		return ErrEmptyAction
	}
	if !s.Source.IsValid() {
		return ErrInvalidSource
	}
	if !s.Category.IsValid() {
		return ErrInvalidCategory
	}
	if err := ValidateConfidence(s.Confidence); err != nil {
		return err
	}
	if err := ValidateSentiment(s.Sentiment); err != nil {
		return err
	}
	return nil
}
