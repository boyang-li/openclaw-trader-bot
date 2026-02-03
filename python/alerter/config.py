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


def get_env_list(key: str, default: list[str] | None = None) -> list[str]:
    value = os.environ.get(key)
    if value is None:
        return default or []
    return [item.strip() for item in value.split(",") if item.strip()]


@dataclass
class KafkaConfig:
    brokers: list[str]
    input_topic: str
    consumer_group: str
    auto_offset_reset: str
    enable_auto_commit: bool
    session_timeout_ms: int
    heartbeat_interval_ms: int

    @classmethod
    def from_env(cls) -> "KafkaConfig":
        brokers_str = get_env("KAFKA_BROKERS", "localhost:9092")
        brokers = [b.strip() for b in brokers_str.split(",") if b.strip()]
        
        return cls(
            brokers=brokers,
            input_topic=get_env("INPUT_TOPIC", "l1.signals.enriched"),
            consumer_group=get_env("CONSUMER_GROUP", "alerter"),
            auto_offset_reset=get_env("AUTO_OFFSET_RESET", "latest"),
            enable_auto_commit=get_env_bool("ENABLE_AUTO_COMMIT", True),
            session_timeout_ms=get_env_int("SESSION_TIMEOUT_MS", 30000),
            heartbeat_interval_ms=get_env_int("HEARTBEAT_INTERVAL_MS", 10000),
        )


@dataclass
class TelegramConfig:
    enabled: bool
    bot_token: str
    chat_id: str
    parse_mode: str
    disable_notification: bool

    @classmethod
    def from_env(cls) -> "TelegramConfig":
        return cls(
            enabled=get_env_bool("TELEGRAM_ENABLED", True),
            bot_token=get_env("TELEGRAM_BOT_TOKEN", ""),
            chat_id=get_env("TELEGRAM_CHAT_ID", ""),
            parse_mode=get_env("TELEGRAM_PARSE_MODE", "HTML"),
            disable_notification=get_env_bool("TELEGRAM_SILENT", False),
        )

    def is_configured(self) -> bool:
        return bool(self.enabled and self.bot_token and self.chat_id)


@dataclass
class DiscordConfig:
    enabled: bool
    webhook_url: str

    @classmethod
    def from_env(cls) -> "DiscordConfig":
        return cls(
            enabled=get_env_bool("DISCORD_ENABLED", False),
            webhook_url=get_env("DISCORD_WEBHOOK_URL", ""),
        )

    def is_configured(self) -> bool:
        return bool(self.enabled and self.webhook_url)


@dataclass
class FilterConfig:
    min_urgency: str
    min_sentiment_magnitude: float
    alert_market_impacts: list[str]
    alert_categories: list[str]
    alert_sources: list[str]
    rate_limit_per_minute: int
    dedup_window_seconds: int

    @classmethod
    def from_env(cls) -> "FilterConfig":
        return cls(
            min_urgency=get_env("MIN_URGENCY", "high"),
            min_sentiment_magnitude=get_env_float("MIN_SENTIMENT_MAGNITUDE", 0.5),
            alert_market_impacts=get_env_list("ALERT_MARKET_IMPACTS", 
                ["positive", "negative", "highly_positive", "highly_negative"]),
            alert_categories=get_env_list("ALERT_CATEGORIES", []),
            alert_sources=get_env_list("ALERT_SOURCES", []),
            rate_limit_per_minute=get_env_int("RATE_LIMIT_PER_MINUTE", 30),
            dedup_window_seconds=get_env_int("DEDUP_WINDOW_SECONDS", 300),
        )


@dataclass
class AppConfig:
    log_level: str
    worker_id: str
    dry_run: bool

    @classmethod
    def from_env(cls) -> "AppConfig":
        import socket
        hostname = socket.gethostname()
        
        return cls(
            log_level=get_env("LOG_LEVEL", "INFO"),
            worker_id=get_env("WORKER_ID", f"alerter-{hostname}"),
            dry_run=get_env_bool("DRY_RUN", False),
        )


@dataclass
class Config:
    kafka: KafkaConfig
    telegram: TelegramConfig
    discord: DiscordConfig
    filter: FilterConfig
    app: AppConfig

    @classmethod
    def from_env(cls) -> "Config":
        return cls(
            kafka=KafkaConfig.from_env(),
            telegram=TelegramConfig.from_env(),
            discord=DiscordConfig.from_env(),
            filter=FilterConfig.from_env(),
            app=AppConfig.from_env(),
        )

    def log_config(self):
        logger.info("=== Alerter Configuration ===")
        logger.info(f"Kafka brokers: {self.kafka.brokers}")
        logger.info(f"Input topic: {self.kafka.input_topic}")
        logger.info(f"Consumer group: {self.kafka.consumer_group}")
        logger.info(f"Telegram enabled: {self.telegram.enabled} (configured: {self.telegram.is_configured()})")
        logger.info(f"Discord enabled: {self.discord.enabled} (configured: {self.discord.is_configured()})")
        logger.info(f"Min urgency: {self.filter.min_urgency}")
        logger.info(f"Min sentiment magnitude: {self.filter.min_sentiment_magnitude}")
        logger.info(f"Alert market impacts: {self.filter.alert_market_impacts}")
        logger.info(f"Rate limit: {self.filter.rate_limit_per_minute}/min")
        logger.info(f"Dedup window: {self.filter.dedup_window_seconds}s")
        logger.info(f"Dry run: {self.app.dry_run}")
        logger.info(f"Worker ID: {self.app.worker_id}")
        logger.info("=============================")
