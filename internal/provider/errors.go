package provider

import (
	"errors"
	"fmt"
)

var (
	ErrProviderAlreadyRegistered = errors.New("provider: already registered")
	ErrProviderNotFound          = errors.New("provider: not found")
	ErrProviderStopped           = errors.New("provider: stopped")
	ErrNoHandlers                = errors.New("provider: no handlers registered")
	ErrHandlerFailed             = errors.New("provider: handler failed")
)

type ProviderError struct {
	Provider string
	Op       string
	Err      error
}

func (e *ProviderError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("provider %s: %s: %v", e.Provider, e.Op, e.Err)
	}
	return fmt.Sprintf("provider %s: %s", e.Provider, e.Op)
}

func (e *ProviderError) Unwrap() error {
	return e.Err
}
