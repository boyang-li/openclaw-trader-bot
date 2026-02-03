"""
Configuration module - loads settings from environment variables.
================================================================
"""

import os
from dataclasses import dataclass
from typing import Optional
import logging

logger = logging.getLogger(__name__)


def get_env(key: str, default: str = "") -> str:
    """Get environment variable with default."""
    return os.environ.get(key, default)


def get_env_int(key: str, default: int) -> int:
    """Get environment variable as integer."""
    value = os.environ.get(key)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        logger.warning(f"Invalid integer for {key}: {value}, using default {default}")
        return default


def get_env_float(key: str, default: float) -> float:
    """Get environment variable as float."""
    value = os.environ.get(key)
    if value is None:
        return default
    try:
        return float(value)
    except ValueError:
        logger.warning(f"Invalid float for {key}: {value}, using default {default}")
        return default


def get_env_bool(key: str, default: bool) -> bool:
    """Get environment variable as boolean."""
    value = os.environ.get(key)
    if value is None:
        return default
    return value.lower() in ("true", "1", "yes", "on")


def get_env_list(key: str, default: list[str] | None = None) -> list[str]:
    """Get environment variable as comma-separated list."""
    value = os.environ.get(key)
    if value is None:
        return default or []
    return [item.strip() for item in value.split(",") if item.strip()]


@dataclass
class KafkaConfig:
    """Kafka connection configuration."""
    brokers: list[str]
    input_topic: str
    output_topic: str
    dlq_topic: str
    consumer_group: str
    auto_offset_reset: str
    enable_auto_commit: bool
    max_poll_records: int
    session_timeout_ms: int
    heartbeat_interval_ms: int

    @classmethod
    def from_env(cls) -> "KafkaConfig":
        brokers_str = get_env("KAFKA_BROKERS", "localhost:9092")
        brokers = [b.strip() for b in brokers_str.split(",") if b.strip()]
        
        return cls(
            brokers=brokers,
            input_topic=get_env("INPUT_TOPIC", "l1.signals.raw"),
            output_topic=get_env("OUTPUT_TOPIC", "l1.signals.enriched"),
            dlq_topic=get_env("DLQ_TOPIC", "l1.signals.dlq"),
            consumer_group=get_env("CONSUMER_GROUP", "slm-worker"),
            auto_offset_reset=get_env("AUTO_OFFSET_RESET", "earliest"),
            enable_auto_commit=get_env_bool("ENABLE_AUTO_COMMIT", False),
            max_poll_records=get_env_int("MAX_POLL_RECORDS", 10),
            session_timeout_ms=get_env_int("SESSION_TIMEOUT_MS", 30000),
            heartbeat_interval_ms=get_env_int("HEARTBEAT_INTERVAL_MS", 10000),
        )


@dataclass
class SLMConfig:
    """SLM (Small Language Model) configuration."""
    model_name: str
    device: str  # "cpu", "cuda", "mps" (Apple Silicon)
    max_length: int
    batch_size: int
    processing_timeout: float
    temperature: float
    use_4bit: bool  # Quantization for memory efficiency
    trust_remote_code: bool

    @classmethod
    def from_env(cls) -> "SLMConfig":
        return cls(
            model_name=get_env("MODEL_NAME", "Qwen/Qwen2.5-1.5B-Instruct"),
            device=get_env("DEVICE", "auto"),  # auto-detect
            max_length=get_env_int("MAX_LENGTH", 512),
            batch_size=get_env_int("BATCH_SIZE", 5),
            processing_timeout=get_env_float("PROCESSING_TIMEOUT", 30.0),
            temperature=get_env_float("TEMPERATURE", 0.1),
            use_4bit=get_env_bool("USE_4BIT", True),
            trust_remote_code=get_env_bool("TRUST_REMOTE_CODE", True),
        )


@dataclass
class AppConfig:
    """Application-level configuration."""
    log_level: str
    metrics_port: int
    health_port: int
    worker_id: str
    max_retries: int
    retry_delay_ms: int

    @classmethod
    def from_env(cls) -> "AppConfig":
        import socket
        hostname = socket.gethostname()
        
        return cls(
            log_level=get_env("LOG_LEVEL", "INFO"),
            metrics_port=get_env_int("METRICS_PORT", 8080),
            health_port=get_env_int("HEALTH_PORT", 8080),
            worker_id=get_env("WORKER_ID", f"slm-worker-{hostname}"),
            max_retries=get_env_int("MAX_RETRIES", 3),
            retry_delay_ms=get_env_int("RETRY_DELAY_MS", 1000),
        )


@dataclass
class Config:
    """Main configuration container."""
    kafka: KafkaConfig
    slm: SLMConfig
    app: AppConfig

    @classmethod
    def from_env(cls) -> "Config":
        return cls(
            kafka=KafkaConfig.from_env(),
            slm=SLMConfig.from_env(),
            app=AppConfig.from_env(),
        )

    def log_config(self):
        """Log configuration (without secrets)."""
        logger.info("=== SLM Worker Configuration ===")
        logger.info(f"Kafka brokers: {self.kafka.brokers}")
        logger.info(f"Input topic: {self.kafka.input_topic}")
        logger.info(f"Output topic: {self.kafka.output_topic}")
        logger.info(f"DLQ topic: {self.kafka.dlq_topic}")
        logger.info(f"Consumer group: {self.kafka.consumer_group}")
        logger.info(f"SLM model: {self.slm.model_name}")
        logger.info(f"SLM device: {self.slm.device}")
        logger.info(f"Batch size: {self.slm.batch_size}")
        logger.info(f"4-bit quantization: {self.slm.use_4bit}")
        logger.info(f"Worker ID: {self.app.worker_id}")
        logger.info(f"Log level: {self.app.log_level}")
        logger.info("================================")
