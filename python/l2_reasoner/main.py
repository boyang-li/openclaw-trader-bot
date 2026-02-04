"""
L2 Reasoner Main - Kafka consumer loop with correlation engine.
"""

import asyncio
import json
import logging
import signal
import sys
import time
from typing import Dict
from datetime import datetime, timezone

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError
from aiohttp import web
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from .config import Config
from .engine.correlation import CorrelationEngine
from .engine.entity_tracker import EntityTracker, extract_entities
from .persistence.database import DatabaseManager
from .schemas.insight import Insight
from .metrics import get_metrics

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("l2_reasoner")


def validate_signal(signal_data: Dict[str, object]) -> list[str]:
    errors: list[str] = []
    if not signal_data.get("subject"):
        errors.append("subject is required")
    if not signal_data.get("action"):
        errors.append("action is required")
    if not signal_data.get("source"):
        errors.append("source is required")
    if not signal_data.get("category"):
        errors.append("category is required")
    confidence = _safe_float(signal_data.get("confidence", 0.0), None)
    if confidence is None or not (0.0 <= confidence <= 1.0):
        errors.append(f"confidence must be 0.0-1.0, got {signal_data.get('confidence')}")
    sentiment = _safe_float(signal_data.get("sentiment", 0.0), None)
    if sentiment is None or not (-1.0 <= sentiment <= 1.0):
        errors.append(f"sentiment must be -1.0 to 1.0, got {signal_data.get('sentiment')}")
    return errors


def _safe_float(value: object | None, default: float | None = 0.0) -> float | None:
    if value is None:
        return default
    if isinstance(value, (float, int)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return default
    return default


class L2Reasoner:
    def __init__(self, config: Config):
        self.config: Config = config
        self.db: DatabaseManager = DatabaseManager(config.db)
        self.tracker: EntityTracker = EntityTracker(config.correlation.correlation_window_hours)
        self.engine: CorrelationEngine = CorrelationEngine(
            config.correlation.correlation_window_hours
        )
        self.metrics = get_metrics()
        self.consumer: AIOKafkaConsumer | None = None
        self.producer: AIOKafkaProducer | None = None
        self._running: bool = False
        self._ready: bool = False
        self._health_runner: web.AppRunner | None = None
        self._processed_count: int = 0
        self._insight_count: int = 0
        self._error_count: int = 0
        self._last_cleanup: datetime = datetime.now(timezone.utc)

    async def start(self) -> None:
        logger.info("Starting L2 Reasoner...")
        self.config.log_config()

        self.db.initialize()
        self._hydrate_tracker()
        await self._start_health_server()
        await self._connect_kafka()

        self._running = True
        self._ready = True
        logger.info("L2 Reasoner started, entering main loop")

        try:
            await self._consume_loop()
        finally:
            await self._shutdown()

    def _hydrate_tracker(self) -> None:
        try:
            recent_signals = self.db.load_recent_signals(
                self.config.correlation.correlation_window_hours
            )
            self.tracker.load_signals(recent_signals)
            logger.info(f"Loaded {len(recent_signals)} recent signals for replay")
        except Exception as exc:
            logger.error(f"Failed to load recent signals: {exc}")

    async def _start_health_server(self) -> None:
        app = web.Application()
        _ = app.router.add_get("/health", self._health_handler)
        _ = app.router.add_get("/ready", self._ready_handler)
        _ = app.router.add_get("/stats", self._stats_handler)
        _ = app.router.add_get("/metrics", self._metrics_handler)

        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, self.config.app.health_host, self.config.app.health_port)
        await site.start()
        self._health_runner = runner
        logger.info(
            f"Health server started on {self.config.app.health_host}:{self.config.app.health_port}"
        )

    async def _health_handler(self, _request: web.Request) -> web.Response:
        return web.json_response({"status": "healthy"})

    async def _ready_handler(self, _request: web.Request) -> web.Response:
        status = "ready" if self._ready else "not_ready"
        http_status = 200 if self._ready else 503
        return web.json_response({"status": status}, status=http_status)

    async def _stats_handler(self, _request: web.Request) -> web.Response:
        stats = self.db.get_stats()
        stats.update(
            {
                "processed": self._processed_count,
                "insights_emitted": self._insight_count,
                "errors": self._error_count,
            }
        )
        return web.json_response(stats)

    async def _metrics_handler(self, _request: web.Request) -> web.Response:
        """Prometheus metrics endpoint."""
        metrics_data = generate_latest()
        return web.Response(
            body=metrics_data,
            headers={"Content-Type": CONTENT_TYPE_LATEST}
        )

    async def _connect_kafka(self) -> None:
        max_retries = self.config.app.max_retries
        retry_delay = self.config.app.retry_delay_ms / 1000.0

        for attempt in range(max_retries):
            try:
                logger.info(f"Connecting to Kafka (attempt {attempt + 1}/{max_retries})...")

                self.consumer = AIOKafkaConsumer(
                    self.config.kafka.input_topic,
                    bootstrap_servers=",".join(self.config.kafka.brokers),
                    group_id=self.config.kafka.consumer_group,
                    auto_offset_reset=self.config.kafka.auto_offset_reset,
                    enable_auto_commit=self.config.kafka.enable_auto_commit,
                    max_poll_records=self.config.kafka.max_poll_records,
                    session_timeout_ms=self.config.kafka.session_timeout_ms,
                    heartbeat_interval_ms=self.config.kafka.heartbeat_interval_ms,
                )

                self.producer = AIOKafkaProducer(
                    bootstrap_servers=",".join(self.config.kafka.brokers),
                    value_serializer=lambda v: v.encode("utf-8") if isinstance(v, str) else v,
                    key_serializer=lambda k: k.encode("utf-8") if k else None,
                )

                await self.consumer.start()
                await self.producer.start()
                logger.info("Connected to Kafka successfully")
                return
            except KafkaConnectionError as exc:
                logger.warning(f"Kafka connection failed: {exc}")
                if attempt < max_retries - 1:
                    logger.info(f"Retrying in {retry_delay}s...")
                    await asyncio.sleep(retry_delay)
                else:
                    raise RuntimeError(
                        f"Failed to connect to Kafka after {max_retries} attempts"
                    )

    async def _consume_loop(self) -> None:
        if not self.consumer:
            raise RuntimeError("Consumer not initialized")

        async for msg in self.consumer:
            if not self._running:
                break

            process_start = datetime.now(timezone.utc)

            try:
                if isinstance(msg.value, bytes):
                    raw_value = msg.value.decode("utf-8")
                elif msg.value is None:
                    raw_value = ""
                else:
                    raw_value = str(msg.value)
                signal_data = json.loads(raw_value)
                if not isinstance(signal_data, dict):
                    await self._send_to_dlq(raw_value, "Invalid signal payload (not an object)")
                    continue

                errors = validate_signal(signal_data)
                if errors:
                    logger.warning(f"Invalid signal: {errors}")
                    await self._send_to_dlq(raw_value, f"Validation errors: {errors}")
                    self.metrics.record_signal_failed("validation")
                    continue

                entities = extract_entities(signal_data)
                signal_data["entities"] = entities

                signal_id = str(signal_data.get("id", ""))
                if not signal_id:
                    await self._send_to_dlq(raw_value, "Missing signal id")
                    self.metrics.record_signal_failed("missing_id")
                    continue

                if self.db.is_duplicate(signal_id):
                    logger.debug(f"Duplicate signal {signal_id}, skipping")
                    continue

                inserted = self.db.insert_batch([signal_data])
                if inserted == 0:
                    await self._send_to_dlq(raw_value, "Failed to persist signal")
                    self.metrics.record_signal_failed("db_insert")
                    continue
                
                # Record metrics
                source = signal_data.get("source", "unknown")
                category = signal_data.get("category", "unknown")
                self.metrics.record_signal_received(source)
                self.metrics.record_category_signal(category)
                self.metrics.record_kafka_consumed()
                
                entity_keys = self.tracker.add_signal(signal_data)

                processing_latency_ms = (
                    datetime.now(timezone.utc) - process_start
                ).total_seconds() * 1000.0

                for entity_key in entity_keys:
                    signals = self.tracker.get_entity_signals(entity_key)
                    entity_display = self.tracker.get_display_name(entity_key)
                    
                    # Record entity tracking metrics
                    self.metrics.record_entity_tracked(entity_display, len(signals))
                    self.metrics.record_window_signals(len(signals))
                    
                    # Evaluate correlation with timing
                    evaluation_start = time.time()
                    insight = self.engine.evaluate_entity(
                        entity_display=entity_display,
                        signals=signals,
                        processing_latency_ms=processing_latency_ms,
                    )
                    evaluation_latency = time.time() - evaluation_start
                    
                    if insight and insight.metrics:
                        # Record rule evaluation metrics
                        self.metrics.record_rule_evaluation(
                            rule_name="multi_source_confirmation",
                            triggered=True,
                            severity=insight.severity,
                            latency=evaluation_latency
                        )
                        self.metrics.record_insight_generated(
                            insight_type=insight.type,
                            severity=insight.severity
                        )
                        self.metrics.record_correlation_score(insight.metrics.correlation_score)
                        await self._send_insight(insight)
                    else:
                        self.metrics.record_rule_evaluation(
                            rule_name="multi_source_confirmation",
                            triggered=False,
                            latency=evaluation_latency
                        )

                if not self.config.kafka.enable_auto_commit:
                    await self.consumer.commit()

                self._processed_count += 1
                self.metrics.record_signal_processed(source)
                self.metrics.record_processing_latency(processing_latency_ms / 1000.0)  # Convert to seconds
                self._maybe_cleanup_dedup()

            except json.JSONDecodeError as exc:
                logger.error(f"Failed to decode message: {exc}")
                raw_str = msg.value.decode("utf-8") if isinstance(msg.value, bytes) else str(msg.value or "")
                await self._send_to_dlq(raw_str, f"JSON decode error: {exc}")
                self._error_count += 1
            except Exception as exc:
                logger.error(f"Error processing message: {exc}", exc_info=True)
                raw_str = msg.value.decode("utf-8") if isinstance(msg.value, bytes) else str(msg.value or "")
                await self._send_to_dlq(raw_str, f"Processing error: {exc}")
                self._error_count += 1

    def _maybe_cleanup_dedup(self) -> None:
        hours_since_cleanup = (
            datetime.now(timezone.utc) - self._last_cleanup
        ).total_seconds() / 3600
        if hours_since_cleanup >= 1:
            try:
                self.db.cleanup_dedup_table(self.config.db.dedup_window_hours)
                self._last_cleanup = datetime.now(timezone.utc)
            except Exception as exc:
                logger.error(f"Dedup cleanup failed: {exc}")

    async def _send_insight(self, insight: Insight) -> None:
        if not self.producer:
            raise RuntimeError("Producer not initialized")
        await self.producer.send_and_wait(
            self.config.kafka.output_insights_topic,
            key=insight.kafka_key(),
            value=insight.to_json(),
        )
        self._insight_count += 1
        self.metrics.record_kafka_produced(self.config.kafka.output_insights_topic)

    async def _send_to_dlq(self, raw_message: str, error: str) -> None:
        if not self.producer:
            logger.error("Producer not initialized, cannot send to DLQ")
            return
        dlq_envelope = {
            "original_message": raw_message,
            "error": error,
            "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "worker_id": self.config.app.worker_id,
        }
        await self.producer.send_and_wait(
            self.config.kafka.dlq_topic,
            value=json.dumps(dlq_envelope),
        )
        self.metrics.record_kafka_produced(self.config.kafka.dlq_topic)

    async def _shutdown(self) -> None:
        logger.info("Shutting down L2 Reasoner...")
        self._running = False
        self._ready = False

        if self.consumer:
            await self.consumer.stop()
        if self.producer:
            await self.producer.stop()

        if self._health_runner:
            await self._health_runner.cleanup()

        stats = self.db.get_stats()
        self.db.close()
        logger.info(
            f"L2 Reasoner stopped. Stats: processed={self._processed_count}, "
            f"insights={self._insight_count}, errors={self._error_count}, "
            f"db={stats.get('session_stats', {})}"
        )

    def stop(self) -> None:
        self._running = False


async def main() -> None:
    config = Config.from_env()
    logging.getLogger().setLevel(config.app.log_level.upper())

    reasoner = L2Reasoner(config)
    loop = asyncio.get_event_loop()

    def handle_signal(sig: signal.Signals) -> None:
        logger.info(f"Received signal {sig}, shutting down...")
        reasoner.stop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda s=sig: handle_signal(s))

    await reasoner.start()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        sys.exit(0)
