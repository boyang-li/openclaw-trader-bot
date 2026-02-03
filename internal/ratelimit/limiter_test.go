package ratelimit

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewLimiter(t *testing.T) {
	l := NewLimiter(10, 5)

	assert.NotNil(t, l)
	assert.Equal(t, 5, l.Burst())
}

func TestLimiter_Allow(t *testing.T) {
	l := NewLimiter(1, 3)

	assert.True(t, l.Allow())
	assert.True(t, l.Allow())
	assert.True(t, l.Allow())
	assert.False(t, l.Allow())
}

func TestLimiter_AllowN(t *testing.T) {
	l := NewLimiter(10, 5)

	assert.True(t, l.AllowN(3))
	assert.True(t, l.AllowN(2))
	assert.False(t, l.AllowN(1))
}

func TestLimiter_Wait(t *testing.T) {
	l := NewLimiter(100, 1)
	l.Allow()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := l.Wait(ctx)
	elapsed := time.Since(start)

	assert.NoError(t, err)
	assert.True(t, elapsed >= 5*time.Millisecond)
}

func TestLimiter_Wait_ContextCancelled(t *testing.T) {
	l := NewLimiter(0.1, 1)
	l.Allow()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	err := l.Wait(ctx)
	assert.Error(t, err)
}

func TestLimiter_Reserve(t *testing.T) {
	l := NewLimiter(10, 2)

	r := l.Reserve()
	assert.NotNil(t, r)
	assert.True(t, r.OK())
}

func TestLimiter_Tokens(t *testing.T) {
	l := NewLimiter(10, 5)

	tokens := l.Tokens()
	assert.True(t, tokens >= 4.9 && tokens <= 5.0)
}

func TestKeyedLimiter_New(t *testing.T) {
	kl := NewKeyedLimiter(10, 5)
	defer kl.Close()

	assert.NotNil(t, kl)
	assert.Equal(t, 0, kl.Len())
}

func TestKeyedLimiter_Allow(t *testing.T) {
	kl := NewKeyedLimiter(1, 2)
	defer kl.Close()

	assert.True(t, kl.Allow("key1"))
	assert.True(t, kl.Allow("key1"))
	assert.False(t, kl.Allow("key1"))

	assert.True(t, kl.Allow("key2"))
	assert.True(t, kl.Allow("key2"))
	assert.False(t, kl.Allow("key2"))
}

func TestKeyedLimiter_SeparateLimitersPerKey(t *testing.T) {
	kl := NewKeyedLimiter(1, 1)
	defer kl.Close()

	assert.True(t, kl.Allow("key1"))
	assert.False(t, kl.Allow("key1"))

	assert.True(t, kl.Allow("key2"))
	assert.False(t, kl.Allow("key2"))

	assert.Equal(t, 2, kl.Len())
}

func TestKeyedLimiter_Wait(t *testing.T) {
	kl := NewKeyedLimiter(100, 1)
	defer kl.Close()

	kl.Allow("key1")

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err := kl.Wait(ctx, "key1")
	assert.NoError(t, err)
}

func TestKeyedLimiter_Cleanup(t *testing.T) {
	kl := NewKeyedLimiterWithIdleTimeout(10, 5, 50*time.Millisecond)
	defer kl.Close()

	kl.Allow("key1")
	kl.Allow("key2")
	assert.Equal(t, 2, kl.Len())

	time.Sleep(100 * time.Millisecond)
	kl.cleanup()

	assert.Equal(t, 0, kl.Len())
}

func TestKeyedLimiter_CleanupKeepsActive(t *testing.T) {
	kl := NewKeyedLimiterWithIdleTimeout(10, 5, 100*time.Millisecond)
	defer kl.Close()

	kl.Allow("key1")
	time.Sleep(50 * time.Millisecond)

	kl.Allow("key1")
	kl.cleanup()

	assert.Equal(t, 1, kl.Len())
}

func TestKeyedLimiter_ConcurrentAccess(t *testing.T) {
	kl := NewKeyedLimiter(1000, 100)
	defer kl.Close()

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(key string) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				kl.Allow(key)
			}
		}("key" + string(rune('0'+i)))
	}

	wg.Wait()
	assert.Equal(t, 10, kl.Len())
}

func TestKeyedLimiter_Close(t *testing.T) {
	kl := NewKeyedLimiter(10, 5)

	kl.Close()
	kl.Close()
}

func TestKeyedLimiter_AllowN(t *testing.T) {
	kl := NewKeyedLimiter(10, 10)
	defer kl.Close()

	assert.True(t, kl.AllowN("key1", 5))
	assert.True(t, kl.AllowN("key1", 5))
	assert.False(t, kl.AllowN("key1", 1))
}

func TestKeyedLimiter_WaitN(t *testing.T) {
	kl := NewKeyedLimiter(100, 5)
	defer kl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err := kl.WaitN(ctx, "key1", 3)
	require.NoError(t, err)

	err = kl.WaitN(ctx, "key1", 2)
	require.NoError(t, err)
}
