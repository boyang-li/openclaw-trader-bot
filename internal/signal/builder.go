package signal

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type Builder struct {
	sig Signal
	err error
}

func NewBuilder(source Source, category Category) *Builder {
	return &Builder{
		sig: Signal{
			ID:        uuid.New(),
			Timestamp: time.Now().UTC(),
			Source:    source,
			Category:  category,
			Urgency:   UrgencyLow,
			Metadata: Metadata{
				IngestedAt:    time.Now().UTC(),
				SchemaVersion: SchemaVersion,
			},
		},
	}
}

func (b *Builder) WithTimestamp(t time.Time) *Builder {
	if b.err != nil {
		return b
	}
	b.sig.Timestamp = t
	return b
}

func (b *Builder) WithSubject(subject string) *Builder {
	if b.err != nil {
		return b
	}
	b.sig.Subject = subject
	return b
}

func (b *Builder) WithAction(action string) *Builder {
	if b.err != nil {
		return b
	}
	b.sig.Action = action
	return b
}

func (b *Builder) WithObject(object string) *Builder {
	if b.err != nil {
		return b
	}
	b.sig.Object = object
	return b
}

func (b *Builder) WithConfidence(conf float64) *Builder {
	if b.err != nil {
		return b
	}
	if err := ValidateConfidence(conf); err != nil {
		b.err = err
		return b
	}
	b.sig.Confidence = conf
	return b
}

func (b *Builder) WithSentiment(sent float64) *Builder {
	if b.err != nil {
		return b
	}
	if err := ValidateSentiment(sent); err != nil {
		b.err = err
		return b
	}
	b.sig.Sentiment = sent
	return b
}

func (b *Builder) WithUrgency(u UrgencyLevel) *Builder {
	if b.err != nil {
		return b
	}
	b.sig.Urgency = u
	return b
}

func (b *Builder) WithTags(tags ...string) *Builder {
	if b.err != nil {
		return b
	}
	b.sig.Tags = append(b.sig.Tags, tags...)
	return b
}

func (b *Builder) WithRawData(data any) *Builder {
	if b.err != nil {
		return b
	}
	raw, err := json.Marshal(data)
	if err != nil {
		b.err = err
		return b
	}
	b.sig.RawData = raw
	return b
}

func (b *Builder) WithProviderMeta(meta map[string]any) *Builder {
	if b.err != nil {
		return b
	}
	b.sig.Metadata.ProviderMeta = meta
	return b
}

func (b *Builder) WithTraceID(id string) *Builder {
	if b.err != nil {
		return b
	}
	b.sig.Metadata.TraceID = id
	return b
}

func (b *Builder) Build() (Signal, error) {
	if b.err != nil {
		return Signal{}, b.err
	}
	if err := b.sig.Validate(); err != nil {
		return Signal{}, err
	}
	return b.sig, nil
}
