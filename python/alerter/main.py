import asyncio
import json
import logging
import signal
import sys
from typing import Any

from aiokafka import AIOKafkaConsumer

from .config import Config
from .filter import SignalFilter
from .notifier import (
    TelegramNotifier,
    DiscordNotifier,
    ConsoleNotifier,
    CompositeNotifier,
    Notifier,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


class AlerterService:
    def __init__(self, config: Config):
        self.config = config
        self.consumer: AIOKafkaConsumer | None = None
        self.signal_filter = SignalFilter(config.filter)
        self.notifier = self._build_notifier()
        self._shutdown_event = asyncio.Event()
        self._stats = {"processed": 0, "alerted": 0, "filtered": 0, "errors": 0}

    def _build_notifier(self) -> Notifier:
        notifiers: list[Notifier] = []
        
        notifiers.append(ConsoleNotifier())
        
        if self.config.telegram.is_configured():
            notifiers.append(TelegramNotifier(self.config.telegram))
            logger.info("Telegram notifier enabled")
        
        if self.config.discord.is_configured():
            notifiers.append(DiscordNotifier(self.config.discord))
            logger.info("Discord notifier enabled")
        
        if len(notifiers) == 1:
            return notifiers[0]
        return CompositeNotifier(notifiers)

    async def start(self):
        logger.info("Starting Alerter Service...")
        self.config.log_config()
        
        self.consumer = AIOKafkaConsumer(
            self.config.kafka.input_topic,
            bootstrap_servers=self.config.kafka.brokers,
            group_id=self.config.kafka.consumer_group,
            auto_offset_reset=self.config.kafka.auto_offset_reset,
            enable_auto_commit=self.config.kafka.enable_auto_commit,
            session_timeout_ms=self.config.kafka.session_timeout_ms,
            heartbeat_interval_ms=self.config.kafka.heartbeat_interval_ms,
        )
        
        await self.consumer.start()
        logger.info(f"Consumer started, subscribed to {self.config.kafka.input_topic}")
        
        try:
            await self._consume_loop()
        finally:
            await self.consumer.stop()
            logger.info(f"Alerter stopped. Stats: {self._stats}")

    async def _consume_loop(self):
        async for message in self.consumer:
            if self._shutdown_event.is_set():
                break
            
            try:
                await self._process_message(message)
            except Exception as e:
                self._stats["errors"] += 1
                logger.error(f"Error processing message: {e}", exc_info=True)

    async def _process_message(self, message):
        self._stats["processed"] += 1
        
        try:
            signal_data = json.loads(message.value.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            logger.warning(f"Failed to parse message: {e}")
            return

        signal_id = signal_data.get("id", "unknown")
        subject = signal_data.get("subject", "unknown")
        
        filter_result = self.signal_filter.should_alert(signal_data)
        
        if not filter_result.should_alert:
            self._stats["filtered"] += 1
            if filter_result.suppression_reason:
                logger.debug(f"Signal {signal_id} filtered: {filter_result.suppression_reason}")
            return

        logger.info(f"Alert triggered for {subject}: {filter_result.reasons}")
        
        if self.config.app.dry_run:
            logger.info(f"[DRY RUN] Would send alert for {subject}")
            self._stats["alerted"] += 1
            return

        success = await self.notifier.send(signal_data, filter_result.reasons)
        if success:
            self._stats["alerted"] += 1
        else:
            self._stats["errors"] += 1

    def shutdown(self):
        logger.info("Shutdown requested...")
        self._shutdown_event.set()


async def main():
    config = Config.from_env()
    
    log_level = getattr(logging, config.app.log_level.upper(), logging.INFO)
    logging.getLogger().setLevel(log_level)
    
    service = AlerterService(config)
    
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, service.shutdown)
    
    await service.start()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        sys.exit(0)
