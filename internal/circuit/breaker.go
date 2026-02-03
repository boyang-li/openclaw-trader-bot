// Package circuit provides circuit breaker functionality for L1 ingestors.
package circuit

import (
	"errors"
	"sync"
	"time"
)

var ErrCircuitOpen = errors.New("circuit breaker is open")

type State int

const (
	StateClosed State = iota
	StateOpen
	StateHalfOpen
)

func (s State) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

type Config struct {
	Name             string
	FailureThreshold int
	SuccessThreshold int
	Timeout          time.Duration
	HalfOpenRequests int
	OnStateChange    func(name string, from, to State)
}

func DefaultConfig(name string) Config {
	return Config{
		Name:             name,
		FailureThreshold: 5,
		SuccessThreshold: 2,
		Timeout:          30 * time.Second,
		HalfOpenRequests: 3,
	}
}

type Counts struct {
	Requests         int64
	Successes        int64
	Failures         int64
	ConsecutiveFails int64
	ConsecutiveSuccs int64
	HalfOpenRequests int64
}

type Breaker struct {
	mu     sync.Mutex
	config Config
	state  State

	counts          Counts
	lastStateChange time.Time
	openedAt        time.Time
}

func New(cfg Config) *Breaker {
	if cfg.FailureThreshold <= 0 {
		cfg.FailureThreshold = 5
	}
	if cfg.SuccessThreshold <= 0 {
		cfg.SuccessThreshold = 2
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 30 * time.Second
	}
	if cfg.HalfOpenRequests <= 0 {
		cfg.HalfOpenRequests = 3
	}

	return &Breaker{
		config:          cfg,
		state:           StateClosed,
		lastStateChange: time.Now(),
	}
}

func (b *Breaker) Execute(fn func() error) error {
	if err := b.beforeRequest(); err != nil {
		return err
	}

	err := fn()
	b.afterRequest(err)
	return err
}

func (b *Breaker) beforeRequest() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := time.Now()

	switch b.state {
	case StateClosed:
		b.counts.Requests++
		return nil

	case StateOpen:
		if now.Sub(b.openedAt) > b.config.Timeout {
			b.transitionTo(StateHalfOpen)
			b.counts.Requests++
			b.counts.HalfOpenRequests = 1
			return nil
		}
		return ErrCircuitOpen

	case StateHalfOpen:
		if b.counts.HalfOpenRequests >= int64(b.config.HalfOpenRequests) {
			return ErrCircuitOpen
		}
		b.counts.Requests++
		b.counts.HalfOpenRequests++
		return nil
	}

	return nil
}

func (b *Breaker) afterRequest(err error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if err != nil {
		b.onFailure()
	} else {
		b.onSuccess()
	}
}

func (b *Breaker) onSuccess() {
	b.counts.Successes++
	b.counts.ConsecutiveSuccs++
	b.counts.ConsecutiveFails = 0

	switch b.state {
	case StateHalfOpen:
		if b.counts.ConsecutiveSuccs >= int64(b.config.SuccessThreshold) {
			b.transitionTo(StateClosed)
		}
	}
}

func (b *Breaker) onFailure() {
	b.counts.Failures++
	b.counts.ConsecutiveFails++
	b.counts.ConsecutiveSuccs = 0

	switch b.state {
	case StateClosed:
		if b.counts.ConsecutiveFails >= int64(b.config.FailureThreshold) {
			b.transitionTo(StateOpen)
		}
	case StateHalfOpen:
		b.transitionTo(StateOpen)
	}
}

func (b *Breaker) transitionTo(newState State) {
	if b.state == newState {
		return
	}

	oldState := b.state
	b.state = newState
	b.lastStateChange = time.Now()

	if newState == StateOpen {
		b.openedAt = time.Now()
	}

	if newState == StateClosed {
		b.counts.ConsecutiveFails = 0
		b.counts.ConsecutiveSuccs = 0
	}

	if newState == StateHalfOpen {
		b.counts.ConsecutiveSuccs = 0
		b.counts.HalfOpenRequests = 0
	}

	if b.config.OnStateChange != nil {
		go b.config.OnStateChange(b.config.Name, oldState, newState)
	}
}

func (b *Breaker) State() State {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}

func (b *Breaker) Counts() Counts {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.counts
}

func (b *Breaker) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.state = StateClosed
	b.counts = Counts{}
	b.lastStateChange = time.Now()
}

func (b *Breaker) Name() string {
	return b.config.Name
}
