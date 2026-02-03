package provider

import (
	"context"
	"sync"

	"go.uber.org/zap"
)

type Registry struct {
	providers map[string]Provider
	mu        sync.RWMutex
	logger    *zap.Logger
}

func NewRegistry(logger *zap.Logger) *Registry {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &Registry{
		providers: make(map[string]Provider),
		logger:    logger,
	}
}

func (r *Registry) Register(p Provider) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.providers[p.Name()]; exists {
		return ErrProviderAlreadyRegistered
	}

	r.providers[p.Name()] = p
	r.logger.Info("provider registered", zap.String("name", p.Name()))
	return nil
}

func (r *Registry) Get(name string) (Provider, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	p, ok := r.providers[name]
	return p, ok
}

func (r *Registry) List() []Provider {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make([]Provider, 0, len(r.providers))
	for _, p := range r.providers {
		result = append(result, p)
	}
	return result
}

func (r *Registry) StartAll(ctx context.Context) error {
	r.mu.RLock()
	providers := make([]Provider, 0, len(r.providers))
	for _, p := range r.providers {
		providers = append(providers, p)
	}
	r.mu.RUnlock()

	var wg sync.WaitGroup
	errCh := make(chan error, len(providers))

	for _, p := range providers {
		wg.Add(1)
		go func(p Provider) {
			defer wg.Done()
			r.logger.Info("starting provider", zap.String("name", p.Name()))
			if err := p.Start(ctx); err != nil {
				r.logger.Error("provider start failed", zap.String("name", p.Name()), zap.Error(err))
				errCh <- &ProviderError{Provider: p.Name(), Op: "start", Err: err}
			}
		}(p)
	}

	go func() {
		wg.Wait()
		close(errCh)
	}()

	select {
	case err := <-errCh:
		if err != nil {
			return err
		}
	case <-ctx.Done():
		return ctx.Err()
	}

	return nil
}

func (r *Registry) StopAll() error {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var lastErr error
	for name, p := range r.providers {
		r.logger.Info("stopping provider", zap.String("name", name))
		if err := p.Stop(); err != nil {
			r.logger.Error("provider stop failed", zap.String("name", name), zap.Error(err))
			lastErr = err
		}
	}
	return lastErr
}

func (r *Registry) HealthAll() map[string]HealthStatus {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make(map[string]HealthStatus, len(r.providers))
	for name, p := range r.providers {
		result[name] = p.Health()
	}
	return result
}
