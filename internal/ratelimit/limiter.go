// Package ratelimit provides rate limiting functionality for L1 ingestors.
package ratelimit

import (
	"context"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

type Limiter struct {
	limiter *rate.Limiter
}

func NewLimiter(ratePerSecond float64, burst int) *Limiter {
	return &Limiter{
		limiter: rate.NewLimiter(rate.Limit(ratePerSecond), burst),
	}
}

func (l *Limiter) Allow() bool {
	return l.limiter.Allow()
}

func (l *Limiter) AllowN(n int) bool {
	return l.limiter.AllowN(time.Now(), n)
}

func (l *Limiter) Wait(ctx context.Context) error {
	return l.limiter.Wait(ctx)
}

func (l *Limiter) WaitN(ctx context.Context, n int) error {
	return l.limiter.WaitN(ctx, n)
}

func (l *Limiter) Reserve() *rate.Reservation {
	return l.limiter.Reserve()
}

func (l *Limiter) ReserveN(n int) *rate.Reservation {
	return l.limiter.ReserveN(time.Now(), n)
}

func (l *Limiter) Tokens() float64 {
	return l.limiter.Tokens()
}

func (l *Limiter) Burst() int {
	return l.limiter.Burst()
}

func (l *Limiter) Limit() rate.Limit {
	return l.limiter.Limit()
}

type KeyedLimiter struct {
	mu          sync.RWMutex
	limiters    map[string]*entry
	rateLimit   float64
	burst       int
	idleTimeout time.Duration
	cleanupDone chan struct{}
	cleanupOnce sync.Once
}

type entry struct {
	limiter  *Limiter
	lastUsed time.Time
}

func NewKeyedLimiter(ratePerSecond float64, burst int) *KeyedLimiter {
	kl := &KeyedLimiter{
		limiters:    make(map[string]*entry),
		rateLimit:   ratePerSecond,
		burst:       burst,
		idleTimeout: 10 * time.Minute,
		cleanupDone: make(chan struct{}),
	}

	go kl.cleanupLoop()
	return kl
}

func NewKeyedLimiterWithIdleTimeout(ratePerSecond float64, burst int, idleTimeout time.Duration) *KeyedLimiter {
	kl := &KeyedLimiter{
		limiters:    make(map[string]*entry),
		rateLimit:   ratePerSecond,
		burst:       burst,
		idleTimeout: idleTimeout,
		cleanupDone: make(chan struct{}),
	}

	go kl.cleanupLoop()
	return kl
}

func (kl *KeyedLimiter) getLimiter(key string) *Limiter {
	kl.mu.RLock()
	e, exists := kl.limiters[key]
	kl.mu.RUnlock()

	if exists {
		kl.mu.Lock()
		e.lastUsed = time.Now()
		kl.mu.Unlock()
		return e.limiter
	}

	kl.mu.Lock()
	defer kl.mu.Unlock()

	if e, exists := kl.limiters[key]; exists {
		e.lastUsed = time.Now()
		return e.limiter
	}

	limiter := NewLimiter(kl.rateLimit, kl.burst)
	kl.limiters[key] = &entry{
		limiter:  limiter,
		lastUsed: time.Now(),
	}
	return limiter
}

func (kl *KeyedLimiter) Allow(key string) bool {
	return kl.getLimiter(key).Allow()
}

func (kl *KeyedLimiter) AllowN(key string, n int) bool {
	return kl.getLimiter(key).AllowN(n)
}

func (kl *KeyedLimiter) Wait(ctx context.Context, key string) error {
	return kl.getLimiter(key).Wait(ctx)
}

func (kl *KeyedLimiter) WaitN(ctx context.Context, key string, n int) error {
	return kl.getLimiter(key).WaitN(ctx, n)
}

func (kl *KeyedLimiter) cleanupLoop() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			kl.cleanup()
		case <-kl.cleanupDone:
			return
		}
	}
}

func (kl *KeyedLimiter) cleanup() {
	kl.mu.Lock()
	defer kl.mu.Unlock()

	now := time.Now()
	for key, e := range kl.limiters {
		if now.Sub(e.lastUsed) > kl.idleTimeout {
			delete(kl.limiters, key)
		}
	}
}

func (kl *KeyedLimiter) Close() {
	kl.cleanupOnce.Do(func() {
		close(kl.cleanupDone)
	})
}

func (kl *KeyedLimiter) Len() int {
	kl.mu.RLock()
	defer kl.mu.RUnlock()
	return len(kl.limiters)
}
