#!/usr/bin/env bash
# =============================================================================
# L1 Ingestion Pipeline - Local Development Setup Script
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# =============================================================================
# Check Prerequisites
# =============================================================================
check_prerequisites() {
    log_info "Checking prerequisites..."
    
    local missing=()
    
    if ! command -v docker &> /dev/null; then
        missing+=("docker")
    fi
    
    if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null; then
        missing+=("docker-compose")
    fi
    
    if ! command -v go &> /dev/null; then
        missing+=("go")
    fi
    
    if [ ${#missing[@]} -ne 0 ]; then
        log_error "Missing required tools: ${missing[*]}"
        log_info "Please install the missing tools and try again."
        exit 1
    fi
    
    log_success "All prerequisites met"
}

# =============================================================================
# Setup Environment
# =============================================================================
setup_env() {
    log_info "Setting up environment..."
    
    if [ ! -f "$PROJECT_ROOT/.env" ]; then
        if [ -f "$PROJECT_ROOT/.env.example" ]; then
            cp "$PROJECT_ROOT/.env.example" "$PROJECT_ROOT/.env"
            log_success "Created .env file from .env.example"
            log_warn "Please edit .env file with your API keys before running the services"
        else
            log_error ".env.example not found"
            exit 1
        fi
    else
        log_info ".env file already exists, skipping..."
    fi
}

# =============================================================================
# Build Go Binaries
# =============================================================================
build_go() {
    log_info "Building Go binaries..."
    
    cd "$PROJECT_ROOT"
    
    # Download dependencies
    go mod tidy
    go mod download
    
    # Build all binaries
    if make build; then
        log_success "Go binaries built successfully"
    else
        log_warn "Build failed - this may be expected if cmd/ is not yet implemented"
    fi
}

# =============================================================================
# Start Infrastructure
# =============================================================================
start_infra() {
    log_info "Starting infrastructure services..."
    
    cd "$PROJECT_ROOT/deploy"
    
    # Use docker compose (v2) or docker-compose (v1)
    if docker compose version &> /dev/null; then
        COMPOSE_CMD="docker compose"
    else
        COMPOSE_CMD="docker-compose"
    fi
    
    $COMPOSE_CMD up -d redpanda redis
    
    log_info "Waiting for Redpanda to be ready..."
    sleep 10
    
    # Initialize topics
    $COMPOSE_CMD up redpanda-init
    
    # Start monitoring
    $COMPOSE_CMD up -d prometheus grafana
    
    log_success "Infrastructure services started"
}

# =============================================================================
# Stop Infrastructure
# =============================================================================
stop_infra() {
    log_info "Stopping infrastructure services..."
    
    cd "$PROJECT_ROOT/deploy"
    
    if docker compose version &> /dev/null; then
        docker compose down
    else
        docker-compose down
    fi
    
    log_success "Infrastructure services stopped"
}

# =============================================================================
# Show Status
# =============================================================================
show_status() {
    log_info "Service Status:"
    echo ""
    
    cd "$PROJECT_ROOT/deploy"
    
    if docker compose version &> /dev/null; then
        docker compose ps
    else
        docker-compose ps
    fi
    
    echo ""
    log_info "Access URLs:"
    echo "  - Redpanda Console: http://localhost:8080 (if enabled)"
    echo "  - Prometheus:       http://localhost:9090"
    echo "  - Grafana:          http://localhost:3000 (admin/admin)"
    echo "  - Redpanda Kafka:   localhost:9092"
    echo "  - Redpanda Admin:   localhost:9644"
    echo "  - Redis:            localhost:6379"
    echo "  - Persister API:    http://localhost:8088"
}

# =============================================================================
# Clean Up
# =============================================================================
cleanup() {
    log_info "Cleaning up..."
    
    cd "$PROJECT_ROOT/deploy"
    
    if docker compose version &> /dev/null; then
        docker compose down -v --remove-orphans
    else
        docker-compose down -v --remove-orphans
    fi
    
    log_success "Cleanup complete"
}

# =============================================================================
# Main
# =============================================================================
usage() {
    echo "Usage: $0 {setup|start|stop|status|clean|build}"
    echo ""
    echo "Commands:"
    echo "  setup   - Initial setup (check prerequisites, create .env, build)"
    echo "  start   - Start all infrastructure services"
    echo "  stop    - Stop all infrastructure services"
    echo "  status  - Show service status and access URLs"
    echo "  clean   - Stop services and remove volumes"
    echo "  build   - Build Go binaries"
}

case "${1:-}" in
    setup)
        check_prerequisites
        setup_env
        build_go
        log_success "Setup complete! Run '$0 start' to start services."
        ;;
    start)
        start_infra
        show_status
        ;;
    stop)
        stop_infra
        ;;
    status)
        show_status
        ;;
    clean)
        cleanup
        ;;
    build)
        build_go
        ;;
    *)
        usage
        exit 1
        ;;
esac
