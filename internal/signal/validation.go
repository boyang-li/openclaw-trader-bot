package signal

import "errors"

var (
	ErrEmptySubject      = errors.New("signal: subject cannot be empty")
	ErrEmptyAction       = errors.New("signal: action cannot be empty")
	ErrInvalidSource     = errors.New("signal: invalid source")
	ErrInvalidCategory   = errors.New("signal: invalid category")
	ErrInvalidConfidence = errors.New("signal: confidence must be between 0 and 1")
	ErrInvalidSentiment  = errors.New("signal: sentiment must be between -1 and 1")
)

func ValidateConfidence(c float64) error {
	if c < 0 || c > 1 {
		return ErrInvalidConfidence
	}
	return nil
}

func ValidateSentiment(s float64) error {
	if s < -1 || s > 1 {
		return ErrInvalidSentiment
	}
	return nil
}
