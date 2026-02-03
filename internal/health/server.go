// Package health provides HTTP health check and metrics endpoints for L1 ingestors.
package health

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

// Status represents the health status of a component.
type Status string

const (
	StatusHealthy   Status = "healthy"
	StatusDegraded  Status = "degraded"
	StatusUnhealthy Status = "unhealthy"
)

// ComponentHealth represents the health of a single component.
type ComponentHealth struct {
	Status    Status    `json:"status"`
	Message   string    `json:"message,omitempty"`
	LastCheck time.Time `json:"last_check"`
}

// HealthResponse is the response structure for health checks.
type HealthResponse struct {
	Status     Status                     `json:"status"`
	Service    string                     `json:"service"`
	Version    string                     `json:"version"`
	Uptime     string                     `json:"uptime"`
	Timestamp  time.Time                  `json:"timestamp"`
	Components map[string]ComponentHealth `json:"components,omitempty"`
}

// ReadyResponse is the response structure for readiness checks.
type ReadyResponse struct {
	Ready   bool   `json:"ready"`
	Message string `json:"message,omitempty"`
}

// Checker is a function that checks the health of a component.
type Checker func(ctx context.Context) ComponentHealth

// Server provides HTTP endpoints for health checks and metrics.
type Server struct {
	serviceName string
	version     string
	port        int
	startTime   time.Time
	server      *http.Server
	logger      *zap.Logger

	mu       sync.RWMutex
	checkers map[string]Checker
	ready    bool
}

// NewServer creates a new health server.
func NewServer(serviceName, version string, port int, logger *zap.Logger) *Server {
	return &Server{
		serviceName: serviceName,
		version:     version,
		port:        port,
		startTime:   time.Now(),
		logger:      logger,
		checkers:    make(map[string]Checker),
		ready:       false,
	}
}

// RegisterChecker adds a health checker for a component.
func (s *Server) RegisterChecker(name string, checker Checker) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.checkers[name] = checker
}

// SetReady sets the readiness status.
func (s *Server) SetReady(ready bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ready = ready
}

// Start starts the HTTP server.
func (s *Server) Start(ctx context.Context) error {
	mux := http.NewServeMux()

	// Health endpoint - for liveness probes
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/healthz", s.handleHealth)

	// Ready endpoint - for readiness probes
	mux.HandleFunc("/ready", s.handleReady)
	mux.HandleFunc("/readyz", s.handleReady)

	// Metrics endpoint - for Prometheus
	mux.Handle("/metrics", promhttp.Handler())

	// Info endpoint
	mux.HandleFunc("/info", s.handleInfo)

	s.server = &http.Server{
		Addr:         fmt.Sprintf(":%d", s.port),
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	s.logger.Info("Starting health server",
		zap.String("service", s.serviceName),
		zap.Int("port", s.port),
	)

	go func() {
		if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			s.logger.Error("Health server error", zap.Error(err))
		}
	}()

	return nil
}

// Stop gracefully stops the HTTP server.
func (s *Server) Stop(ctx context.Context) error {
	if s.server == nil {
		return nil
	}

	s.logger.Info("Stopping health server")
	return s.server.Shutdown(ctx)
}

// handleHealth handles the /health endpoint.
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	s.mu.RLock()
	checkers := make(map[string]Checker, len(s.checkers))
	for k, v := range s.checkers {
		checkers[k] = v
	}
	s.mu.RUnlock()

	components := make(map[string]ComponentHealth)
	overallStatus := StatusHealthy

	for name, checker := range checkers {
		health := checker(ctx)
		components[name] = health

		// Determine overall status (worst wins)
		if health.Status == StatusUnhealthy {
			overallStatus = StatusUnhealthy
		} else if health.Status == StatusDegraded && overallStatus != StatusUnhealthy {
			overallStatus = StatusDegraded
		}
	}

	response := HealthResponse{
		Status:     overallStatus,
		Service:    s.serviceName,
		Version:    s.version,
		Uptime:     time.Since(s.startTime).Round(time.Second).String(),
		Timestamp:  time.Now().UTC(),
		Components: components,
	}

	w.Header().Set("Content-Type", "application/json")

	if overallStatus == StatusUnhealthy {
		w.WriteHeader(http.StatusServiceUnavailable)
	} else {
		w.WriteHeader(http.StatusOK)
	}

	json.NewEncoder(w).Encode(response)
}

// handleReady handles the /ready endpoint.
func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	ready := s.ready
	s.mu.RUnlock()

	response := ReadyResponse{
		Ready: ready,
	}

	if !ready {
		response.Message = "service is not ready to accept traffic"
	}

	w.Header().Set("Content-Type", "application/json")

	if ready {
		w.WriteHeader(http.StatusOK)
	} else {
		w.WriteHeader(http.StatusServiceUnavailable)
	}

	json.NewEncoder(w).Encode(response)
}

// handleInfo handles the /info endpoint.
func (s *Server) handleInfo(w http.ResponseWriter, r *http.Request) {
	info := map[string]interface{}{
		"service":    s.serviceName,
		"version":    s.version,
		"start_time": s.startTime.UTC(),
		"uptime":     time.Since(s.startTime).Round(time.Second).String(),
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(info)
}

// CommonCheckers provides factory functions for common health checks.
type CommonCheckers struct{}

// KafkaChecker creates a health checker for Kafka connectivity.
func (CommonCheckers) KafkaChecker(brokers []string) Checker {
	return func(ctx context.Context) ComponentHealth {
		if len(brokers) == 0 {
			return ComponentHealth{
				Status:    StatusUnhealthy,
				Message:   "no brokers configured",
				LastCheck: time.Now(),
			}
		}

		connected := 0
		var lastErr error
		timeout := 2 * time.Second

		for _, broker := range brokers {
			conn, err := net.DialTimeout("tcp", broker, timeout)
			if err != nil {
				lastErr = err
				continue
			}
			conn.Close()
			connected++
		}

		if connected == 0 {
			return ComponentHealth{
				Status:    StatusUnhealthy,
				Message:   fmt.Sprintf("cannot reach any broker: %v", lastErr),
				LastCheck: time.Now(),
			}
		}

		if connected < len(brokers) {
			return ComponentHealth{
				Status:    StatusDegraded,
				Message:   fmt.Sprintf("connected to %d/%d brokers", connected, len(brokers)),
				LastCheck: time.Now(),
			}
		}

		return ComponentHealth{
			Status:    StatusHealthy,
			Message:   fmt.Sprintf("connected to %d brokers", connected),
			LastCheck: time.Now(),
		}
	}
}

// RedisChecker creates a health checker for Redis connectivity.
func (CommonCheckers) RedisChecker(addr string) Checker {
	return func(ctx context.Context) ComponentHealth {
		if addr == "" {
			return ComponentHealth{
				Status:    StatusUnhealthy,
				Message:   "no address configured",
				LastCheck: time.Now(),
			}
		}

		conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
		if err != nil {
			return ComponentHealth{
				Status:    StatusUnhealthy,
				Message:   fmt.Sprintf("cannot connect: %v", err),
				LastCheck: time.Now(),
			}
		}
		conn.Close()

		return ComponentHealth{
			Status:    StatusHealthy,
			Message:   "connected",
			LastCheck: time.Now(),
		}
	}
}
