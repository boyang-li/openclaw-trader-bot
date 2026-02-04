"""
Configuration module - loads settings from environment variables.
================================================================
"""

import logging
import os
from dataclasses import dataclass

logger = logging.getLogger(__name__)


def get_env(key: str, default: str = "") -> str:
    return os.environ.get(key, default)


def get_env_int(key: str, default: int) -> int:
    value = os.environ.get(key)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        logger.warning(f"Invalid integer for {key}: {value}, using default {default}")
        return default


def get_env_float(key: str, default: float) -> float:
    value = os.environ.get(key)
    if value is None:
        return default
    try:
        return float(value)
    except ValueError:
        logger.warning(f"Invalid float for {key}: {value}, using default {default}")
        return default


def get_env_bool(key: str, default: bool) -> bool:
    value = os.environ.get(key)
    if value is None:
        return default
    return value.lower() in ("true", "1", "yes", "on")


@dataclass
class KafkaConfig:
    brokers: list[str]
    input_topic: str
    output_insights_topic: str
    output_situations_topic: str
    output_entities_topic: str
    dlq_topic: str
    consumer_group: str
    auto_offset_reset: str
    enable_auto_commit: bool
    max_poll_records: int
    session_timeout_ms: int
    heartbeat_interval_ms: int

    @classmethod
    def from_env(cls) -> "KafkaConfig":
        brokers_str = get_env("L2_KAFKA_BROKERS", "localhost:9092")
        brokers = [b.strip() for b in brokers_str.split(",") if b.strip()]
        return cls(
            brokers=brokers,
            input_topic=get_env("L2_KAFKA_INPUT_TOPIC", "l1.signals.enriched"),
            output_insights_topic=get_env("L2_OUTPUT_INSIGHTS_TOPIC", "l2.insights"),
            output_situations_topic=get_env("L2_OUTPUT_SITUATIONS_TOPIC", "l2.situations"),
            output_entities_topic=get_env("L2_OUTPUT_ENTITIES_TOPIC", "l2.entities"),
            dlq_topic=get_env("L2_KAFKA_DLQ_TOPIC", "l1.signals.dlq"),
            consumer_group=get_env("L2_KAFKA_CONSUMER_GROUP", "l2-reasoner"),
            auto_offset_reset=get_env("L2_KAFKA_AUTO_OFFSET_RESET", "earliest"),
            enable_auto_commit=get_env_bool("L2_KAFKA_ENABLE_AUTO_COMMIT", False),
            max_poll_records=get_env_int("L2_KAFKA_MAX_POLL_RECORDS", 50),
            session_timeout_ms=get_env_int("L2_KAFKA_SESSION_TIMEOUT_MS", 30000),
            heartbeat_interval_ms=get_env_int("L2_KAFKA_HEARTBEAT_INTERVAL_MS", 10000),
        )


@dataclass
class DatabaseConfig:
    db_path: str
    journal_mode: str
    busy_timeout_ms: int
    cache_size_pages: int
    vacuum_on_startup: bool
    dedup_window_hours: int

    @classmethod
    def from_env(cls) -> "DatabaseConfig":
        return cls(
            db_path=get_env("L2_DB_PATH", "/data/l2_reasoner.db"),
            journal_mode=get_env("L2_DB_JOURNAL_MODE", "WAL"),
            busy_timeout_ms=get_env_int("L2_DB_BUSY_TIMEOUT_MS", 5000),
            cache_size_pages=get_env_int("L2_DB_CACHE_SIZE_PAGES", 2000),
            vacuum_on_startup=get_env_bool("L2_DB_VACUUM_ON_STARTUP", False),
            dedup_window_hours=get_env_int("L2_DB_DEDUP_WINDOW_HOURS", 24),
        )


@dataclass
class CorrelationConfig:
    correlation_window_hours: int
    anomaly_zscore_threshold: float

    @classmethod
    def from_env(cls) -> "CorrelationConfig":
        return cls(
            correlation_window_hours=get_env_int("L2_CORRELATION_WINDOW_HOURS", 4),
            anomaly_zscore_threshold=get_env_float("L2_ANOMALY_ZSCORE_THRESHOLD", 2.5),
        )


@dataclass
class AppConfig:
    log_level: str
    worker_id: str
    health_host: str
    health_port: int
    max_retries: int
    retry_delay_ms: int

    @classmethod
    def from_env(cls) -> "AppConfig":
        import socket
        hostname = socket.gethostname()
        return cls(
            log_level=get_env("L2_LOG_LEVEL", "INFO"),
            worker_id=get_env("L2_WORKER_ID", f"l2-reasoner-{hostname}"),
            health_host=get_env("L2_HEALTH_HOST", "0.0.0.0"),
            health_port=get_env_int("L2_HEALTH_PORT", 8080),
            max_retries=get_env_int("L2_KAFKA_MAX_RETRIES", 3),
            retry_delay_ms=get_env_int("L2_KAFKA_RETRY_DELAY_MS", 1000),
        )


@dataclass
class Config:
    kafka: KafkaConfig
    db: DatabaseConfig
    correlation: CorrelationConfig
    app: AppConfig

    @classmethod
    def from_env(cls) -> "Config":
        return cls(
            kafka=KafkaConfig.from_env(),
            db=DatabaseConfig.from_env(),
            correlation=CorrelationConfig.from_env(),
            app=AppConfig.from_env(),
        )

    def log_config(self) -> None:
        logger.info("=== L2 Reasoner Configuration ===")
        logger.info(f"Kafka brokers: {self.kafka.brokers}")
        logger.info(f"Input topic: {self.kafka.input_topic}")
        logger.info(f"Insights topic: {self.kafka.output_insights_topic}")
        logger.info(f"Situations topic: {self.kafka.output_situations_topic}")
        logger.info(f"Entities topic: {self.kafka.output_entities_topic}")
        logger.info(f"DLQ topic: {self.kafka.dlq_topic}")
        logger.info(f"Consumer group: {self.kafka.consumer_group}")
        logger.info(f"DB path: {self.db.db_path}")
        logger.info(f"Correlation window: {self.correlation.correlation_window_hours}h")
        logger.info(f"Anomaly z-score threshold: {self.correlation.anomaly_zscore_threshold}")
        logger.info(f"Health endpoint: {self.app.health_host}:{self.app.health_port}")
        logger.info(f"Worker ID: {self.app.worker_id}")
        logger.info(f"Log level: {self.app.log_level}")
        logger.info("================================")
