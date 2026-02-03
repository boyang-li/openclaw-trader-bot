package orchestrator

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/openclaworg/l1-ingestion/internal/provider"
	"github.com/openclaworg/l1-ingestion/internal/signal"
)

type mockProvider struct {
	name        string
	category    signal.Category
	startCalled atomic.Bool
	stopCalled  atomic.Bool
	startErr    error
	stopErr     error
	healthy     bool
	handlers    []provider.SignalHandler
	mu          sync.Mutex
}

func newMockProvider(name string, category signal.Category) *mockProvider {
	return &mockProvider{
		name:     name,
		category: category,
		healthy:  true,
	}
}

func (m *mockProvider) Name() string              { return m.name }
func (m *mockProvider) Category() signal.Category { return m.category }

func (m *mockProvider) Start(ctx context.Context) error {
	m.startCalled.Store(true)
	return m.startErr
}

func (m *mockProvider) Stop() error {
	m.stopCalled.Store(true)
	return m.stopErr
}

func (m *mockProvider) Subscribe(handler provider.SignalHandler) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.handlers = append(m.handlers, handler)
	return nil
}

func (m *mockProvider) Health() provider.HealthStatus {
	return provider.HealthStatus{Healthy: m.healthy}
}

func (m *mockProvider) setHealthy(healthy bool) {
	m.healthy = healthy
}

func TestNew(t *testing.T) {
	cfg := DefaultConfig()
	o := New(cfg, nil)

	if o == nil {
		t.Fatal("expected non-nil orchestrator")
	}
	if o.State() != StateStopped {
		t.Errorf("expected initial state StateStopped, got %v", o.State())
	}
}

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.ShutdownTimeout != 30*time.Second {
		t.Errorf("expected ShutdownTimeout 30s, got %v", cfg.ShutdownTimeout)
	}
	if cfg.StartupTimeout != 60*time.Second {
		t.Errorf("expected StartupTimeout 60s, got %v", cfg.StartupTimeout)
	}
	if cfg.EnableSupervision {
		t.Error("expected EnableSupervision to be false by default")
	}
}

func TestRegister(t *testing.T) {
	o := New(DefaultConfig(), nil)
	p := newMockProvider("test", signal.CategoryCrypto)

	if err := o.Register(p); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	providers := o.ListProviders()
	if len(providers) != 1 {
		t.Fatalf("expected 1 provider, got %d", len(providers))
	}
	if providers[0].Name != "test" {
		t.Errorf("expected provider name 'test', got %q", providers[0].Name)
	}
}

func TestRegister_Duplicate(t *testing.T) {
	o := New(DefaultConfig(), nil)
	p1 := newMockProvider("test", signal.CategoryCrypto)
	p2 := newMockProvider("test", signal.CategoryMacro)

	if err := o.Register(p1); err != nil {
		t.Fatalf("unexpected error on first register: %v", err)
	}

	if err := o.Register(p2); err == nil {
		t.Error("expected error when registering duplicate provider")
	}
}

func TestRegister_WhileRunning(t *testing.T) {
	o := New(DefaultConfig(), nil)
	p1 := newMockProvider("test1", signal.CategoryCrypto)
	o.Register(p1)

	ctx := context.Background()
	if err := o.Start(ctx); err != nil {
		t.Fatalf("failed to start: %v", err)
	}
	defer o.Stop()

	p2 := newMockProvider("test2", signal.CategoryMacro)
	if err := o.Register(p2); err == nil {
		t.Error("expected error when registering while running")
	}
}

func TestStart_NoProviders(t *testing.T) {
	o := New(DefaultConfig(), nil)
	ctx := context.Background()

	if err := o.Start(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer o.Stop()

	if o.State() != StateRunning {
		t.Errorf("expected StateRunning, got %v", o.State())
	}
}

func TestStart_WithProviders(t *testing.T) {
	o := New(DefaultConfig(), nil)
	p1 := newMockProvider("test1", signal.CategoryCrypto)
	p2 := newMockProvider("test2", signal.CategoryMacro)

	o.Register(p1)
	o.Register(p2)

	ctx := context.Background()
	if err := o.Start(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer o.Stop()

	time.Sleep(10 * time.Millisecond)

	if !p1.startCalled.Load() {
		t.Error("expected p1.Start() to be called")
	}
	if !p2.startCalled.Load() {
		t.Error("expected p2.Start() to be called")
	}
	if o.State() != StateRunning {
		t.Errorf("expected StateRunning, got %v", o.State())
	}
}

func TestStart_ProviderError(t *testing.T) {
	o := New(DefaultConfig(), nil)
	p := newMockProvider("failing", signal.CategoryCrypto)
	p.startErr = errors.New("start failed")

	o.Register(p)

	ctx := context.Background()
	if err := o.Start(ctx); err == nil {
		t.Error("expected error when provider fails to start")
	}

	if o.State() != StateStopped {
		t.Errorf("expected StateStopped after failed start, got %v", o.State())
	}
}

func TestStart_AlreadyRunning(t *testing.T) {
	o := New(DefaultConfig(), nil)
	ctx := context.Background()

	if err := o.Start(ctx); err != nil {
		t.Fatalf("first start failed: %v", err)
	}
	defer o.Stop()

	if err := o.Start(ctx); err == nil {
		t.Error("expected error when starting already running orchestrator")
	}
}

func TestStop(t *testing.T) {
	o := New(DefaultConfig(), nil)
	p := newMockProvider("test", signal.CategoryCrypto)
	o.Register(p)

	ctx := context.Background()
	if err := o.Start(ctx); err != nil {
		t.Fatalf("start failed: %v", err)
	}

	if err := o.Stop(); err != nil {
		t.Fatalf("stop failed: %v", err)
	}

	if o.State() != StateStopped {
		t.Errorf("expected StateStopped, got %v", o.State())
	}
	if !p.stopCalled.Load() {
		t.Error("expected provider Stop() to be called")
	}
}

func TestStop_NotRunning(t *testing.T) {
	o := New(DefaultConfig(), nil)

	if err := o.Stop(); err != nil {
		t.Errorf("unexpected error stopping non-running orchestrator: %v", err)
	}
}

func TestHealth(t *testing.T) {
	o := New(DefaultConfig(), nil)
	p1 := newMockProvider("healthy", signal.CategoryCrypto)
	p2 := newMockProvider("unhealthy", signal.CategoryMacro)
	p2.setHealthy(false)

	o.Register(p1)
	o.Register(p2)

	health := o.Health()
	if len(health) != 2 {
		t.Fatalf("expected 2 health entries, got %d", len(health))
	}

	if !health["healthy"].Healthy {
		t.Error("expected 'healthy' provider to be healthy")
	}
	if health["unhealthy"].Healthy {
		t.Error("expected 'unhealthy' provider to be unhealthy")
	}
}

func TestHealthSummary(t *testing.T) {
	o := New(DefaultConfig(), nil)
	p1 := newMockProvider("healthy", signal.CategoryCrypto)
	p2 := newMockProvider("unhealthy", signal.CategoryMacro)
	p2.setHealthy(false)

	o.Register(p1)
	o.Register(p2)

	summary := o.HealthSummary()

	if summary.TotalProviders != 2 {
		t.Errorf("expected 2 total providers, got %d", summary.TotalProviders)
	}
	if summary.HealthyCount != 1 {
		t.Errorf("expected 1 healthy, got %d", summary.HealthyCount)
	}
	if summary.UnhealthyCount != 1 {
		t.Errorf("expected 1 unhealthy, got %d", summary.UnhealthyCount)
	}
	if summary.State != StateStopped {
		t.Errorf("expected StateStopped, got %v", summary.State)
	}
}

func TestSetGlobalHandler(t *testing.T) {
	o := New(DefaultConfig(), nil)
	p := newMockProvider("test", signal.CategoryCrypto)
	o.Register(p)

	o.SetGlobalHandler(func(ctx context.Context, sig signal.Signal) error {
		return nil
	})

	ctx := context.Background()
	if err := o.Start(ctx); err != nil {
		t.Fatalf("start failed: %v", err)
	}
	defer o.Stop()

	if len(p.handlers) != 1 {
		t.Errorf("expected 1 handler subscribed, got %d", len(p.handlers))
	}
}

func TestStateString(t *testing.T) {
	tests := []struct {
		state    State
		expected string
	}{
		{StateStopped, "stopped"},
		{StateStarting, "starting"},
		{StateRunning, "running"},
		{StateStopping, "stopping"},
		{State(99), "unknown"},
	}

	for _, tt := range tests {
		if got := tt.state.String(); got != tt.expected {
			t.Errorf("State(%d).String() = %q, want %q", tt.state, got, tt.expected)
		}
	}
}

func TestSetStateChangeCallback(t *testing.T) {
	o := New(DefaultConfig(), nil)

	var transitions []struct{ from, to State }
	o.SetStateChangeCallback(func(old, new State) {
		transitions = append(transitions, struct{ from, to State }{old, new})
	})

	ctx := context.Background()
	o.Start(ctx)
	o.Stop()

	if len(transitions) < 2 {
		t.Fatalf("expected at least 2 transitions, got %d", len(transitions))
	}
}

func TestGetProvider(t *testing.T) {
	o := New(DefaultConfig(), nil)
	p := newMockProvider("test", signal.CategoryCrypto)
	o.Register(p)

	got, ok := o.GetProvider("test")
	if !ok {
		t.Fatal("expected provider to be found")
	}
	if got.Name() != "test" {
		t.Errorf("expected provider name 'test', got %q", got.Name())
	}

	_, ok = o.GetProvider("nonexistent")
	if ok {
		t.Error("expected provider to not be found")
	}
}

func TestGetCircuitBreaker(t *testing.T) {
	o := New(DefaultConfig(), nil)
	p := newMockProvider("test", signal.CategoryCrypto)
	o.Register(p)

	cb, ok := o.GetCircuitBreaker("test")
	if !ok {
		t.Fatal("expected circuit breaker to be found")
	}
	if cb == nil {
		t.Error("expected non-nil circuit breaker")
	}

	_, ok = o.GetCircuitBreaker("nonexistent")
	if ok {
		t.Error("expected circuit breaker to not be found")
	}
}

func TestRestartTracker(t *testing.T) {
	rt := &restartTracker{}

	for i := 0; i < 3; i++ {
		if !rt.addRestart(time.Minute, 3) {
			t.Errorf("restart %d should be allowed", i+1)
		}
	}

	if rt.addRestart(time.Minute, 3) {
		t.Error("4th restart should not be allowed within window")
	}
}

func TestRestartTracker_WindowExpiry(t *testing.T) {
	rt := &restartTracker{}

	for i := 0; i < 3; i++ {
		rt.addRestart(time.Minute, 3)
	}

	rt.mu.Lock()
	for i := range rt.restarts {
		rt.restarts[i] = time.Now().Add(-2 * time.Minute)
	}
	rt.mu.Unlock()

	if !rt.addRestart(time.Minute, 3) {
		t.Error("restart should be allowed after window expiry")
	}
}

func TestRateLimiting(t *testing.T) {
	cfg := DefaultConfig()
	cfg.RateLimitPerSecond = 100
	cfg.RateLimitBurst = 10

	o := New(cfg, nil)

	if o.rateLimiter == nil {
		t.Error("expected rate limiter to be initialized")
	}
}

func TestRateLimiting_Disabled(t *testing.T) {
	cfg := DefaultConfig()
	cfg.RateLimitPerSecond = 0

	o := New(cfg, nil)

	if o.rateLimiter != nil {
		t.Error("expected rate limiter to be nil when disabled")
	}
}

func TestListProviders(t *testing.T) {
	o := New(DefaultConfig(), nil)

	providers := o.ListProviders()
	if len(providers) != 0 {
		t.Errorf("expected 0 providers, got %d", len(providers))
	}

	o.Register(newMockProvider("p1", signal.CategoryCrypto))
	o.Register(newMockProvider("p2", signal.CategoryMacro))
	o.Register(newMockProvider("p3", signal.CategoryGeopolitical))

	providers = o.ListProviders()
	if len(providers) != 3 {
		t.Errorf("expected 3 providers, got %d", len(providers))
	}
}
