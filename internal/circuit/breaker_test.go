package circuit

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errTest = errors.New("test error")

func TestNew(t *testing.T) {
	cfg := DefaultConfig("test")
	b := New(cfg)

	assert.NotNil(t, b)
	assert.Equal(t, StateClosed, b.State())
	assert.Equal(t, "test", b.Name())
}

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig("test")

	assert.Equal(t, "test", cfg.Name)
	assert.Equal(t, 5, cfg.FailureThreshold)
	assert.Equal(t, 2, cfg.SuccessThreshold)
	assert.Equal(t, 30*time.Second, cfg.Timeout)
	assert.Equal(t, 3, cfg.HalfOpenRequests)
}

func TestNew_DefaultsInvalidConfig(t *testing.T) {
	cfg := Config{
		Name:             "test",
		FailureThreshold: 0,
		SuccessThreshold: 0,
		Timeout:          0,
		HalfOpenRequests: 0,
	}

	b := New(cfg)

	assert.Equal(t, 5, b.config.FailureThreshold)
	assert.Equal(t, 2, b.config.SuccessThreshold)
	assert.Equal(t, 30*time.Second, b.config.Timeout)
	assert.Equal(t, 3, b.config.HalfOpenRequests)
}

func TestState_String(t *testing.T) {
	assert.Equal(t, "closed", StateClosed.String())
	assert.Equal(t, "open", StateOpen.String())
	assert.Equal(t, "half-open", StateHalfOpen.String())
	assert.Equal(t, "unknown", State(99).String())
}

func TestBreaker_StartsInClosedState(t *testing.T) {
	b := New(DefaultConfig("test"))
	assert.Equal(t, StateClosed, b.State())
}

func TestBreaker_ExecuteSuccess(t *testing.T) {
	b := New(DefaultConfig("test"))

	err := b.Execute(func() error {
		return nil
	})

	assert.NoError(t, err)
	counts := b.Counts()
	assert.Equal(t, int64(1), counts.Requests)
	assert.Equal(t, int64(1), counts.Successes)
	assert.Equal(t, int64(0), counts.Failures)
}

func TestBreaker_ExecuteFailure(t *testing.T) {
	b := New(DefaultConfig("test"))

	err := b.Execute(func() error {
		return errTest
	})

	assert.Error(t, err)
	assert.Equal(t, errTest, err)
	counts := b.Counts()
	assert.Equal(t, int64(1), counts.Requests)
	assert.Equal(t, int64(0), counts.Successes)
	assert.Equal(t, int64(1), counts.Failures)
}

func TestBreaker_OpensAfterFailureThreshold(t *testing.T) {
	cfg := Config{
		Name:             "test",
		FailureThreshold: 3,
		SuccessThreshold: 2,
		Timeout:          30 * time.Second,
		HalfOpenRequests: 2,
	}
	b := New(cfg)

	for i := 0; i < 3; i++ {
		_ = b.Execute(func() error { return errTest })
	}

	assert.Equal(t, StateOpen, b.State())
}

func TestBreaker_OpenRejectsRequests(t *testing.T) {
	cfg := Config{
		Name:             "test",
		FailureThreshold: 1,
		Timeout:          1 * time.Hour,
	}
	b := New(cfg)

	_ = b.Execute(func() error { return errTest })
	assert.Equal(t, StateOpen, b.State())

	executed := false
	err := b.Execute(func() error {
		executed = true
		return nil
	})

	assert.Error(t, err)
	assert.Equal(t, ErrCircuitOpen, err)
	assert.False(t, executed)
}

func TestBreaker_TransitionsToHalfOpenAfterTimeout(t *testing.T) {
	cfg := Config{
		Name:             "test",
		FailureThreshold: 1,
		Timeout:          50 * time.Millisecond,
		HalfOpenRequests: 1,
	}
	b := New(cfg)

	_ = b.Execute(func() error { return errTest })
	assert.Equal(t, StateOpen, b.State())

	time.Sleep(100 * time.Millisecond)

	err := b.Execute(func() error { return nil })
	assert.NoError(t, err)
	assert.Equal(t, StateHalfOpen, b.State())
}

func TestBreaker_HalfOpenClosesAfterSuccessThreshold(t *testing.T) {
	cfg := Config{
		Name:             "test",
		FailureThreshold: 1,
		SuccessThreshold: 2,
		Timeout:          10 * time.Millisecond,
		HalfOpenRequests: 5,
	}
	b := New(cfg)

	_ = b.Execute(func() error { return errTest })
	time.Sleep(20 * time.Millisecond)

	_ = b.Execute(func() error { return nil })
	assert.Equal(t, StateHalfOpen, b.State())

	_ = b.Execute(func() error { return nil })
	assert.Equal(t, StateClosed, b.State())
}

func TestBreaker_HalfOpenReopensOnFailure(t *testing.T) {
	cfg := Config{
		Name:             "test",
		FailureThreshold: 1,
		Timeout:          10 * time.Millisecond,
		HalfOpenRequests: 5,
	}
	b := New(cfg)

	_ = b.Execute(func() error { return errTest })
	time.Sleep(20 * time.Millisecond)

	_ = b.Execute(func() error { return nil })
	assert.Equal(t, StateHalfOpen, b.State())

	_ = b.Execute(func() error { return errTest })
	assert.Equal(t, StateOpen, b.State())
}

func TestBreaker_HalfOpenLimitsRequests(t *testing.T) {
	cfg := Config{
		Name:             "test",
		FailureThreshold: 1,
		SuccessThreshold: 10,
		Timeout:          10 * time.Millisecond,
		HalfOpenRequests: 2,
	}
	b := New(cfg)

	_ = b.Execute(func() error { return errTest })
	time.Sleep(20 * time.Millisecond)

	_ = b.Execute(func() error { return nil })
	_ = b.Execute(func() error { return nil })

	err := b.Execute(func() error { return nil })
	assert.Equal(t, ErrCircuitOpen, err)
}

func TestBreaker_OnStateChangeCallback(t *testing.T) {
	var transitions []struct{ from, to State }
	var mu sync.Mutex

	cfg := Config{
		Name:             "test",
		FailureThreshold: 1,
		Timeout:          10 * time.Millisecond,
		OnStateChange: func(name string, from, to State) {
			mu.Lock()
			transitions = append(transitions, struct{ from, to State }{from, to})
			mu.Unlock()
		},
	}
	b := New(cfg)

	_ = b.Execute(func() error { return errTest })
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	require.Len(t, transitions, 1)
	assert.Equal(t, StateClosed, transitions[0].from)
	assert.Equal(t, StateOpen, transitions[0].to)
	mu.Unlock()
}

func TestBreaker_Reset(t *testing.T) {
	cfg := Config{
		Name:             "test",
		FailureThreshold: 1,
	}
	b := New(cfg)

	_ = b.Execute(func() error { return errTest })
	assert.Equal(t, StateOpen, b.State())

	b.Reset()

	assert.Equal(t, StateClosed, b.State())
	counts := b.Counts()
	assert.Equal(t, int64(0), counts.Requests)
	assert.Equal(t, int64(0), counts.Failures)
}

func TestBreaker_ConcurrentAccess(t *testing.T) {
	cfg := Config{
		Name:             "test",
		FailureThreshold: 100,
	}
	b := New(cfg)

	var successCount int64
	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				err := b.Execute(func() error { return nil })
				if err == nil {
					atomic.AddInt64(&successCount, 1)
				}
			}
		}()
	}

	wg.Wait()

	assert.Equal(t, int64(10000), atomic.LoadInt64(&successCount))
	assert.Equal(t, StateClosed, b.State())
}

func TestBreaker_SuccessResetsConsecutiveFails(t *testing.T) {
	cfg := Config{
		Name:             "test",
		FailureThreshold: 5,
	}
	b := New(cfg)

	for i := 0; i < 4; i++ {
		_ = b.Execute(func() error { return errTest })
	}

	_ = b.Execute(func() error { return nil })

	for i := 0; i < 4; i++ {
		_ = b.Execute(func() error { return errTest })
	}

	assert.Equal(t, StateClosed, b.State())
}
