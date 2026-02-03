package health

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestNewServer(t *testing.T) {
	s := NewServer("test-service", "1.0.0", 8080, zap.NewNop())

	assert.NotNil(t, s)
	assert.Equal(t, "test-service", s.serviceName)
	assert.Equal(t, "1.0.0", s.version)
	assert.Equal(t, 8080, s.port)
	assert.False(t, s.ready)
	assert.Empty(t, s.checkers)
}

func TestRegisterChecker(t *testing.T) {
	s := NewServer("test", "1.0.0", 8080, zap.NewNop())

	checker := func(ctx context.Context) ComponentHealth {
		return ComponentHealth{Status: StatusHealthy}
	}

	s.RegisterChecker("test-component", checker)

	assert.Len(t, s.checkers, 1)
	_, exists := s.checkers["test-component"]
	assert.True(t, exists)
}

func TestSetReady(t *testing.T) {
	s := NewServer("test", "1.0.0", 8080, zap.NewNop())

	assert.False(t, s.ready)

	s.SetReady(true)
	assert.True(t, s.ready)

	s.SetReady(false)
	assert.False(t, s.ready)
}

func TestHandleHealth_NoCheckers(t *testing.T) {
	s := NewServer("test-service", "1.0.0", 8080, zap.NewNop())

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()

	s.handleHealth(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)

	var resp HealthResponse
	err := json.Unmarshal(rec.Body.Bytes(), &resp)
	require.NoError(t, err)

	assert.Equal(t, StatusHealthy, resp.Status)
	assert.Equal(t, "test-service", resp.Service)
	assert.Equal(t, "1.0.0", resp.Version)
	assert.Empty(t, resp.Components)
}

func TestHandleHealth_HealthyCheckers(t *testing.T) {
	s := NewServer("test-service", "1.0.0", 8080, zap.NewNop())

	s.RegisterChecker("kafka", func(ctx context.Context) ComponentHealth {
		return ComponentHealth{
			Status:    StatusHealthy,
			Message:   "connected",
			LastCheck: time.Now(),
		}
	})

	s.RegisterChecker("redis", func(ctx context.Context) ComponentHealth {
		return ComponentHealth{
			Status:    StatusHealthy,
			Message:   "connected",
			LastCheck: time.Now(),
		}
	})

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()

	s.handleHealth(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)

	var resp HealthResponse
	err := json.Unmarshal(rec.Body.Bytes(), &resp)
	require.NoError(t, err)

	assert.Equal(t, StatusHealthy, resp.Status)
	assert.Len(t, resp.Components, 2)
	assert.Equal(t, StatusHealthy, resp.Components["kafka"].Status)
	assert.Equal(t, StatusHealthy, resp.Components["redis"].Status)
}

func TestHandleHealth_DegradedChecker(t *testing.T) {
	s := NewServer("test-service", "1.0.0", 8080, zap.NewNop())

	s.RegisterChecker("kafka", func(ctx context.Context) ComponentHealth {
		return ComponentHealth{Status: StatusHealthy}
	})

	s.RegisterChecker("redis", func(ctx context.Context) ComponentHealth {
		return ComponentHealth{
			Status:  StatusDegraded,
			Message: "partial connectivity",
		}
	})

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()

	s.handleHealth(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)

	var resp HealthResponse
	err := json.Unmarshal(rec.Body.Bytes(), &resp)
	require.NoError(t, err)

	assert.Equal(t, StatusDegraded, resp.Status)
}

func TestHandleHealth_UnhealthyChecker(t *testing.T) {
	s := NewServer("test-service", "1.0.0", 8080, zap.NewNop())

	s.RegisterChecker("kafka", func(ctx context.Context) ComponentHealth {
		return ComponentHealth{Status: StatusHealthy}
	})

	s.RegisterChecker("redis", func(ctx context.Context) ComponentHealth {
		return ComponentHealth{
			Status:  StatusUnhealthy,
			Message: "connection refused",
		}
	})

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()

	s.handleHealth(rec, req)

	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)

	var resp HealthResponse
	err := json.Unmarshal(rec.Body.Bytes(), &resp)
	require.NoError(t, err)

	assert.Equal(t, StatusUnhealthy, resp.Status)
}

func TestHandleReady_Ready(t *testing.T) {
	s := NewServer("test-service", "1.0.0", 8080, zap.NewNop())
	s.SetReady(true)

	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	rec := httptest.NewRecorder()

	s.handleReady(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)

	var resp ReadyResponse
	err := json.Unmarshal(rec.Body.Bytes(), &resp)
	require.NoError(t, err)

	assert.True(t, resp.Ready)
	assert.Empty(t, resp.Message)
}

func TestHandleReady_NotReady(t *testing.T) {
	s := NewServer("test-service", "1.0.0", 8080, zap.NewNop())
	s.SetReady(false)

	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	rec := httptest.NewRecorder()

	s.handleReady(rec, req)

	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)

	var resp ReadyResponse
	err := json.Unmarshal(rec.Body.Bytes(), &resp)
	require.NoError(t, err)

	assert.False(t, resp.Ready)
	assert.NotEmpty(t, resp.Message)
}

func TestHandleInfo(t *testing.T) {
	s := NewServer("test-service", "1.0.0", 8080, zap.NewNop())

	req := httptest.NewRequest(http.MethodGet, "/info", nil)
	rec := httptest.NewRecorder()

	s.handleInfo(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]interface{}
	err := json.Unmarshal(rec.Body.Bytes(), &resp)
	require.NoError(t, err)

	assert.Equal(t, "test-service", resp["service"])
	assert.Equal(t, "1.0.0", resp["version"])
	assert.NotEmpty(t, resp["start_time"])
	assert.NotEmpty(t, resp["uptime"])
}

func TestKafkaChecker_NoBrokers(t *testing.T) {
	checker := CommonCheckers{}.KafkaChecker([]string{})
	health := checker(context.Background())

	assert.Equal(t, StatusUnhealthy, health.Status)
	assert.Contains(t, health.Message, "no brokers configured")
}

func TestKafkaChecker_UnreachableBroker(t *testing.T) {
	checker := CommonCheckers{}.KafkaChecker([]string{"localhost:99999"})
	health := checker(context.Background())

	assert.Equal(t, StatusUnhealthy, health.Status)
	assert.Contains(t, health.Message, "cannot reach any broker")
}

func TestRedisChecker_NoAddress(t *testing.T) {
	checker := CommonCheckers{}.RedisChecker("")
	health := checker(context.Background())

	assert.Equal(t, StatusUnhealthy, health.Status)
	assert.Contains(t, health.Message, "no address configured")
}

func TestRedisChecker_UnreachableAddress(t *testing.T) {
	checker := CommonCheckers{}.RedisChecker("localhost:99999")
	health := checker(context.Background())

	assert.Equal(t, StatusUnhealthy, health.Status)
	assert.Contains(t, health.Message, "cannot connect")
}

func TestStatus_Constants(t *testing.T) {
	assert.Equal(t, Status("healthy"), StatusHealthy)
	assert.Equal(t, Status("degraded"), StatusDegraded)
	assert.Equal(t, Status("unhealthy"), StatusUnhealthy)
}

func TestStartStop(t *testing.T) {
	s := NewServer("test-service", "1.0.0", 0, zap.NewNop())

	ctx := context.Background()
	err := s.Start(ctx)
	require.NoError(t, err)

	time.Sleep(50 * time.Millisecond)

	shutdownCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	err = s.Stop(shutdownCtx)
	assert.NoError(t, err)
}

func TestStop_NilServer(t *testing.T) {
	s := NewServer("test-service", "1.0.0", 8080, zap.NewNop())

	err := s.Stop(context.Background())
	assert.NoError(t, err)
}
