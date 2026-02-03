package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/openclaworg/l1-ingestion/internal/circuit"
	"github.com/openclaworg/l1-ingestion/internal/provider"
	"github.com/openclaworg/l1-ingestion/internal/ratelimit"
	"github.com/openclaworg/l1-ingestion/internal/signal"
	"go.uber.org/zap"
)

type State int32

const (
	StateStopped State = iota
	StateStarting
	StateRunning
	StateStopping
)

func (s State) String() string {
	switch s {
	case StateStopped:
		return "stopped"
	case StateStarting:
		return "starting"
	case StateRunning:
		return "running"
	case StateStopping:
		return "stopping"
	default:
		return "unknown"
	}
}

type Config struct {
	ShutdownTimeout      time.Duration
	StartupTimeout       time.Duration
	HealthCheckInterval  time.Duration
	EnableSupervision    bool
	MaxRestarts          int
	RestartWindow        time.Duration
	RateLimitPerSecond   float64
	RateLimitBurst       int
	CircuitBreakerConfig circuit.Config
}

func DefaultConfig() Config {
	return Config{
		ShutdownTimeout:     30 * time.Second,
		StartupTimeout:      60 * time.Second,
		HealthCheckInterval: 10 * time.Second,
		EnableSupervision:   false,
		MaxRestarts:         3,
		RestartWindow:       5 * time.Minute,
		RateLimitPerSecond:  0,
		RateLimitBurst:      100,
		CircuitBreakerConfig: circuit.Config{
			FailureThreshold: 5,
			SuccessThreshold: 2,
			Timeout:          30 * time.Second,
			HalfOpenRequests: 3,
		},
	}
}

type ProviderInfo struct {
	Name     string
	Category signal.Category
	Enabled  bool
}

type restartTracker struct {
	mu       sync.Mutex
	restarts []time.Time
}

func (rt *restartTracker) addRestart(window time.Duration, maxRestarts int) bool {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	now := time.Now()
	cutoff := now.Add(-window)

	filtered := rt.restarts[:0]
	for _, t := range rt.restarts {
		if t.After(cutoff) {
			filtered = append(filtered, t)
		}
	}
	rt.restarts = filtered

	if len(rt.restarts) >= maxRestarts {
		return false
	}

	rt.restarts = append(rt.restarts, now)
	return true
}

type Orchestrator struct {
	config   Config
	registry *provider.Registry
	logger   *zap.Logger

	state   atomic.Int32
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	startMu sync.Mutex

	providersMu      sync.RWMutex
	providers        map[string]provider.Provider
	providerCircuits map[string]*circuit.Breaker
	restartTrackers  map[string]*restartTracker

	rateLimiter   *ratelimit.Limiter
	globalHandler provider.SignalHandler
	onStateChange func(old, new State)
}

func New(cfg Config, logger *zap.Logger) *Orchestrator {
	if logger == nil {
		logger = zap.NewNop()
	}

	o := &Orchestrator{
		config:           cfg,
		registry:         provider.NewRegistry(logger),
		logger:           logger,
		providers:        make(map[string]provider.Provider),
		providerCircuits: make(map[string]*circuit.Breaker),
		restartTrackers:  make(map[string]*restartTracker),
	}

	if cfg.RateLimitPerSecond > 0 {
		o.rateLimiter = ratelimit.NewLimiter(cfg.RateLimitPerSecond, cfg.RateLimitBurst)
	}

	return o
}

func (o *Orchestrator) SetStateChangeCallback(cb func(old, new State)) {
	o.onStateChange = cb
}

func (o *Orchestrator) SetGlobalHandler(handler provider.SignalHandler) {
	o.globalHandler = handler
}

func (o *Orchestrator) setState(new State) {
	old := State(o.state.Swap(int32(new)))
	if old != new {
		o.logger.Info("orchestrator state changed",
			zap.String("from", old.String()),
			zap.String("to", new.String()),
		)
		if o.onStateChange != nil {
			o.onStateChange(old, new)
		}
	}
}

func (o *Orchestrator) State() State {
	return State(o.state.Load())
}

func (o *Orchestrator) Register(p provider.Provider) error {
	if o.State() != StateStopped {
		return errors.New("cannot register providers while running")
	}

	o.providersMu.Lock()
	defer o.providersMu.Unlock()

	name := p.Name()
	if _, exists := o.providers[name]; exists {
		return fmt.Errorf("provider %q already registered", name)
	}

	cbConfig := o.config.CircuitBreakerConfig
	cbConfig.Name = name
	cbConfig.OnStateChange = func(cbName string, from, to circuit.State) {
		o.logger.Info("provider circuit breaker state changed",
			zap.String("provider", cbName),
			zap.String("from", from.String()),
			zap.String("to", to.String()),
		)
	}
	cb := circuit.New(cbConfig)

	o.providers[name] = p
	o.providerCircuits[name] = cb
	o.restartTrackers[name] = &restartTracker{}

	if err := o.registry.Register(p); err != nil {
		delete(o.providers, name)
		delete(o.providerCircuits, name)
		delete(o.restartTrackers, name)
		return err
	}

	o.logger.Info("provider registered",
		zap.String("name", name),
		zap.String("category", string(p.Category())),
	)

	return nil
}

func (o *Orchestrator) ListProviders() []ProviderInfo {
	o.providersMu.RLock()
	defer o.providersMu.RUnlock()

	result := make([]ProviderInfo, 0, len(o.providers))
	for name, p := range o.providers {
		result = append(result, ProviderInfo{
			Name:     name,
			Category: p.Category(),
			Enabled:  true,
		})
	}
	return result
}

func (o *Orchestrator) Start(ctx context.Context) error {
	o.startMu.Lock()
	defer o.startMu.Unlock()

	if o.State() != StateStopped {
		return errors.New("orchestrator already running")
	}

	o.setState(StateStarting)
	o.ctx, o.cancel = context.WithCancel(ctx)

	o.providersMu.RLock()
	providers := make([]provider.Provider, 0, len(o.providers))
	for _, p := range o.providers {
		providers = append(providers, p)
	}
	o.providersMu.RUnlock()

	if len(providers) == 0 {
		o.setState(StateRunning)
		o.logger.Warn("no providers registered")
		return nil
	}

	if o.globalHandler != nil {
		wrappedHandler := o.wrapHandler(o.globalHandler)
		for _, p := range providers {
			if err := p.Subscribe(wrappedHandler); err != nil {
				o.setState(StateStopped)
				return fmt.Errorf("failed to subscribe handler to %s: %w", p.Name(), err)
			}
		}
	}

	startCtx, startCancel := context.WithTimeout(o.ctx, o.config.StartupTimeout)
	defer startCancel()

	errCh := make(chan error, len(providers))
	var startWg sync.WaitGroup

	for _, p := range providers {
		startWg.Add(1)
		go func(p provider.Provider) {
			defer startWg.Done()
			o.logger.Info("starting provider", zap.String("name", p.Name()))
			if err := p.Start(o.ctx); err != nil {
				errCh <- fmt.Errorf("provider %s failed to start: %w", p.Name(), err)
			}
		}(p)
	}

	done := make(chan struct{})
	go func() {
		startWg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-startCtx.Done():
		o.setState(StateStopped)
		return fmt.Errorf("startup timeout: %w", startCtx.Err())
	}

	close(errCh)
	var startupErrors []error
	for err := range errCh {
		startupErrors = append(startupErrors, err)
	}

	if len(startupErrors) > 0 {
		o.stopAllProviders()
		o.setState(StateStopped)
		return fmt.Errorf("startup failed: %v", startupErrors)
	}

	o.setState(StateRunning)

	if o.config.EnableSupervision {
		o.wg.Add(1)
		go o.supervisionLoop()
	}

	o.logger.Info("orchestrator started", zap.Int("provider_count", len(providers)))
	return nil
}

func (o *Orchestrator) wrapHandler(handler provider.SignalHandler) provider.SignalHandler {
	return func(ctx context.Context, sig signal.Signal) error {
		if o.rateLimiter != nil {
			if err := o.rateLimiter.Wait(ctx); err != nil {
				return fmt.Errorf("rate limit wait: %w", err)
			}
		}

		sourceName := string(sig.Source)
		o.providersMu.RLock()
		cb := o.providerCircuits[sourceName]
		o.providersMu.RUnlock()

		if cb != nil {
			return cb.Execute(func() error {
				return handler(ctx, sig)
			})
		}

		return handler(ctx, sig)
	}
}

func (o *Orchestrator) supervisionLoop() {
	defer o.wg.Done()

	ticker := time.NewTicker(o.config.HealthCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-o.ctx.Done():
			return
		case <-ticker.C:
			o.checkAndRestartUnhealthy()
		}
	}
}

func (o *Orchestrator) checkAndRestartUnhealthy() {
	o.providersMu.RLock()
	providers := make(map[string]provider.Provider)
	for k, v := range o.providers {
		providers[k] = v
	}
	o.providersMu.RUnlock()

	for name, p := range providers {
		health := p.Health()
		if health.Healthy {
			continue
		}

		o.logger.Warn("provider unhealthy",
			zap.String("name", name),
			zap.String("last_error", health.LastError),
			zap.Int64("error_count", health.ErrorCount),
		)

		o.providersMu.RLock()
		tracker := o.restartTrackers[name]
		o.providersMu.RUnlock()

		if tracker == nil {
			continue
		}

		if !tracker.addRestart(o.config.RestartWindow, o.config.MaxRestarts) {
			o.logger.Error("provider exceeded max restarts",
				zap.String("name", name),
				zap.Int("max_restarts", o.config.MaxRestarts),
				zap.Duration("window", o.config.RestartWindow),
			)
			continue
		}

		o.logger.Info("restarting unhealthy provider", zap.String("name", name))

		if err := p.Stop(); err != nil {
			o.logger.Error("failed to stop provider for restart",
				zap.String("name", name),
				zap.Error(err),
			)
		}

		go func(p provider.Provider, name string) {
			if err := p.Start(o.ctx); err != nil {
				o.logger.Error("failed to restart provider",
					zap.String("name", name),
					zap.Error(err),
				)
			}
		}(p, name)
	}
}

func (o *Orchestrator) Stop() error {
	o.startMu.Lock()
	defer o.startMu.Unlock()

	if o.State() == StateStopped {
		return nil
	}

	o.setState(StateStopping)

	if o.cancel != nil {
		o.cancel()
	}

	done := make(chan struct{})
	go func() {
		o.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(o.config.ShutdownTimeout):
		o.logger.Warn("shutdown timeout waiting for supervision loop")
	}

	o.stopAllProviders()
	o.setState(StateStopped)
	o.logger.Info("orchestrator stopped")

	return nil
}

func (o *Orchestrator) stopAllProviders() {
	o.providersMu.RLock()
	providers := make([]provider.Provider, 0, len(o.providers))
	for _, p := range o.providers {
		providers = append(providers, p)
	}
	o.providersMu.RUnlock()

	var wg sync.WaitGroup
	for _, p := range providers {
		wg.Add(1)
		go func(p provider.Provider) {
			defer wg.Done()
			if err := p.Stop(); err != nil {
				o.logger.Error("error stopping provider",
					zap.String("name", p.Name()),
					zap.Error(err),
				)
			}
		}(p)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(o.config.ShutdownTimeout):
		o.logger.Warn("shutdown timeout waiting for providers to stop")
	}
}

func (o *Orchestrator) Health() map[string]provider.HealthStatus {
	return o.registry.HealthAll()
}

type HealthSummary struct {
	State           State                            `json:"state"`
	TotalProviders  int                              `json:"total_providers"`
	HealthyCount    int                              `json:"healthy_count"`
	UnhealthyCount  int                              `json:"unhealthy_count"`
	ProviderHealth  map[string]provider.HealthStatus `json:"provider_health"`
	CircuitBreakers map[string]string                `json:"circuit_breakers"`
}

func (o *Orchestrator) HealthSummary() HealthSummary {
	summary := HealthSummary{
		State:           o.State(),
		ProviderHealth:  o.Health(),
		CircuitBreakers: make(map[string]string),
	}

	summary.TotalProviders = len(summary.ProviderHealth)

	for name, h := range summary.ProviderHealth {
		if h.Healthy {
			summary.HealthyCount++
		} else {
			summary.UnhealthyCount++
		}

		o.providersMu.RLock()
		if cb, ok := o.providerCircuits[name]; ok {
			summary.CircuitBreakers[name] = cb.State().String()
		}
		o.providersMu.RUnlock()
	}

	return summary
}

func (o *Orchestrator) GetProvider(name string) (provider.Provider, bool) {
	o.providersMu.RLock()
	defer o.providersMu.RUnlock()
	p, ok := o.providers[name]
	return p, ok
}

func (o *Orchestrator) GetCircuitBreaker(name string) (*circuit.Breaker, bool) {
	o.providersMu.RLock()
	defer o.providersMu.RUnlock()
	cb, ok := o.providerCircuits[name]
	return cb, ok
}
