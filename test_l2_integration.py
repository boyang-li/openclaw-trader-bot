#!/usr/bin/env python3
"""
Test L2 Reasoner integration without Docker.
"""

import asyncio
import json
import os
import sys
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

# Add L2 Reasoner to path
sys.path.insert(0, 'python/l2_reasoner')

async def test_l2_reasoner_basic():
    """Test basic L2 Reasoner functionality."""
    print("Testing L2 Reasoner integration...")
    
    # Mock environment variables
    os.environ.update({
        'L2_KAFKA_BROKERS': 'localhost:9092',
        'L2_KAFKA_INPUT_TOPIC': 'l1.signals.enriched',
        'L2_KAFKA_CONSUMER_GROUP': 'l2-reasoner-test',
        'L2_OUTPUT_INSIGHTS_TOPIC': 'l2.insights.test',
        'L2_OUTPUT_SITUATIONS_TOPIC': 'l2.situations.test',
        'L2_OUTPUT_ENTITIES_TOPIC': 'l2.entities.test',
        'L2_KAFKA_DLQ_TOPIC': 'l1.signals.dlq.test',
        'L2_DB_PATH': ':memory:',  # Use in-memory SQLite for testing
        'L2_CORRELATION_WINDOW_HOURS': '1',
        'L2_ANOMALY_ZSCORE_THRESHOLD': '2.5',
        'L2_LOG_LEVEL': 'INFO',
        'L2_METRICS_PORT': '8080',
        'L2_HEALTH_PORT': '8080',
    })
    
    try:
        # Import after setting env vars
        from l2_reasoner.config import Config
        from l2_reasoner.main import L2Reasoner
        from l2_reasoner.schemas.insight import Insight
        
        print("✓ Config and imports working")
        
        # Test config loading
        config = Config.from_env()
        print(f"✓ Config loaded: worker_id={config.app.worker_id}")
        print(f"✓ Input topic: {config.kafka.input_topic}")
        print(f"✓ Output insights topic: {config.kafka.output_insights_topic}")
        print(f"✓ DB path: {config.db.db_path}")
        
        # Test schema
        insight = Insight(
            id="test-insight-123",
            timestamp="2024-01-15T10:35:00Z",
            type="multi_source_confirmation",
            severity="warning",
            title="Test insight",
            description="Test description",
            trigger_signals=[],
            entities=["TestEntity"],
            categories=["test"],
            metrics={
                "correlation_score": 0.8,
                "confidence": 0.75,
                "signal_count": 2,
                "time_window_hours": 1.5
            },
            context={},
            metadata={
                "created_at": "2024-01-15T10:35:00Z",
                "schema_version": "2.0.0",
                "reasoner_version": "1.0.0",
                "processing_latency_ms": 100.0
            }
        )
        
        insight_json = insight.to_json()
        print(f"✓ Insight schema working: {len(insight_json)} bytes")
        
        # Test metrics
        from l2_reasoner.metrics import get_metrics
        metrics = get_metrics()
        metrics.record_signal_received("test_source")
        metrics.record_signal_processed("test_source")
        metrics.record_insight_generated("test_type", "warning")
        print("✓ Metrics recording working")
        
        print("\n✅ L2 Reasoner basic integration tests passed!")
        print("\nNext steps:")
        print("1. Start Docker daemon")
        print("2. Run: cd deploy && docker compose --env-file ../.env up -d redpanda l2-reasoner")
        print("3. Verify L2 Reasoner is healthy: curl http://localhost:8082/health")
        print("4. Check metrics: curl http://localhost:8082/metrics")
        print("5. View dashboard: http://localhost:3000 (admin/admin)")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == '__main__':
    success = asyncio.run(test_l2_reasoner_basic())
    sys.exit(0 if success else 1)