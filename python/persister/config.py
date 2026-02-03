"""
Configuration module - loads settings from environment variables.
"""

import os
from dataclasses import dataclass
import logging

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


def get_env_bool(key: str, default: bool) -> bool:
    value = os.environ.get(key)
    if value is None:
        return default
    return value.lower() in ("true", "1", "yes", "on")


@dataclass
class KafkaConfig:
    """Kafka connection configuration."""
    brokers: list[str]
    input_topic: str
    consumer_group: str
    auto_offset_reset: str
    enable_auto_commit: bool
    session_timeout_ms: int
    heartbeat_interval_ms: int
    max_poll_records: int

    @classmethod
    def from_env(cls) -> "KafkaConfig":
        brokers_str = get_env("KAFKA_BROKERS", "localhost:9092")
        brokers = [b.strip() for b in brokers_str.split(",") if b.strip()]
        
        return cls(
            brokers=brokers,
            input_topic=get_env("INPUT_TOPIC", "l1.signals.enriched"),
            consumer_group=get_env("CONSUMER_GROUP", "persister"),
            auto_offset_reset=get_env("AUTO_OFFSET_RESET", "earliest"),
            enable_auto_commit=get_env_bool("ENABLE_AUTO_COMMIT", False),
            session_timeout_ms=get_env_int("SESSION_TIMEOUT_MS", 30000),
            heartbeat_interval_ms=get_env_int("HEARTBEAT_INTERVAL_MS", 10000),
            max_poll_records=get_env_int("MAX_POLL_RECORDS", 100),
        )


@dataclass
class DatabaseConfig:
    """SQLite database configuration."""
    db_path: str
    journal_mode: str  # WAL mode for concurrent reads
    busy_timeout_ms: int
    cache_size_pages: int
    vacuum_on_startup: bool
    dedup_window_hours: int  # How long to keep dedup records
    batch_size: int  # Signals to batch before committing

    @classmethod
    def from_env(cls) -> "DatabaseConfig":
        return cls(
            db_path=get_env("DB_PATH", "/data/signals.db"),
            journal_mode=get_env("DB_JOURNAL_MODE", "WAL"),
            busy_timeout_ms=get_env_int("DB_BUSY_TIMEOUT_MS", 5000),
            cache_size_pages=get_env_int("DB_CACHE_SIZE_PAGES", 2000),
            vacuum_on_startup=get_env_bool("DB_VACUUM_ON_STARTUP", False),
            dedup_window_hours=get_env_int("DEDUP_WINDOW_HOURS", 24),
            batch_size=get_env_int("BATCH_SIZE", 50),
        )


@dataclass
class APIConfig:
    """HTTP API configuration."""
    enabled: bool
    host: str
    port: int
    max_query_results: int

    @classmethod
    def from_env(cls) -> "APIConfig":
        return cls(
            enabled=get_env_bool("API_ENABLED", True),
            host=get_env("API_HOST", "0.0.0.0"),
            port=get_env_int("API_PORT", 8080),
            max_query_results=get_env_int("API_MAX_RESULTS", 1000),
        )


@dataclass
class AppConfig:
    """Application-level configuration."""
    log_level: str
    worker_id: str
    metrics_enabled: bool

    @classmethod
    def from_env(cls) -> "AppConfig":
        import socket
        hostname = socket.gethostname()
        
        return cls(
            log_level=get_env("LOG_LEVEL", "INFO"),
            worker_id=get_env("WORKER_ID", f"persister-{hostname}"),
            metrics_enabled=get_env_bool("METRICS_ENABLED", True),
        )


@dataclass
class Config:
    """Main configuration container."""
    kafka: KafkaConfig
    db: DatabaseConfig
    api: APIConfig
    app: AppConfig

    @classmethod
    def from_env(cls) -> "Config":
        return cls(
            kafka=KafkaConfig.from_env(),
            db=DatabaseConfig.from_env(),
            api=APIConfig.from_env(),
            app=AppConfig.from_env(),
        )

    def log_config(self):
        """Log configuration (without secrets)."""
        logger.info("=== Persister Configuration ===")
        logger.info(f"Kafka brokers: {self.kafka.brokers}")
        logger.info(f"Input topic: {self.kafka.input_topic}")
        logger.info(f"Consumer group: {self.kafka.consumer_group}")
        logger.info(f"Database path: {self.db.db_path}")
        logger.info(f"Journal mode: {self.db.journal_mode}")
        logger.info(f"Batch size: {self.db.batch_size}")
        logger.info(f"Dedup window: {self.db.dedup_window_hours}h")
        logger.info(f"API enabled: {self.api.enabled}")
        if self.api.enabled:
            logger.info(f"API port: {self.api.port}")
        logger.info(f"Worker ID: {self.app.worker_id}")
        logger.info(f"Log level: {self.app.log_level}")
        logger.info("================================")
