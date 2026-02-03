import asyncio
import json
import logging
import signal
import sys
from datetime import datetime

from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaConnectionError

from .config import Config
from .database import DatabaseManager
from .api import QueryAPI

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("persister")


class Persister:
    def __init__(self, config: Config):
        self.config = config
        self.db = DatabaseManager(config.db)
        self.api = QueryAPI(config.api, self.db) if config.api.enabled else None
        self.consumer: AIOKafkaConsumer | None = None
        self._running = False
        self._batch: list[dict] = []
        self._last_cleanup = datetime.utcnow()

    async def start(self):
        logger.info("Starting Persister Service...")
        self.config.log_config()
        
        self.db.initialize()
        
        if self.api:
            await self.api.start()
        
        await self._connect_kafka()
        
        self._running = True
        logger.info("Persister started, entering main loop")
        
        try:
            await self._consume_loop()
        finally:
            await self._shutdown()

    async def _connect_kafka(self):
        max_retries = 10
        retry_delay = 2.0
        
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
                
                await self.consumer.start()
                logger.info("Connected to Kafka successfully")
                return
                
            except KafkaConnectionError as e:
                logger.warning(f"Kafka connection failed: {e}")
                if attempt < max_retries - 1:
                    logger.info(f"Retrying in {retry_delay}s...")
                    await asyncio.sleep(retry_delay)
                else:
                    raise RuntimeError(f"Failed to connect to Kafka after {max_retries} attempts")

    async def _consume_loop(self):
        batch_size = self.config.db.batch_size
        cleanup_interval_hours = 1
        
        if not self.consumer:
            raise RuntimeError("Consumer not initialized")
        
        async for msg in self.consumer:
            if not self._running:
                break
            
            try:
                raw_value = msg.value.decode("utf-8") if isinstance(msg.value, bytes) else msg.value
                signal_data = json.loads(raw_value)
                
                self._batch.append(signal_data)
                
                if len(self._batch) >= batch_size:
                    await self._flush_batch()
                
                hours_since_cleanup = (datetime.utcnow() - self._last_cleanup).total_seconds() / 3600
                if hours_since_cleanup >= cleanup_interval_hours:
                    self._cleanup_dedup_table()
                    
            except json.JSONDecodeError as e:
                logger.error(f"Failed to decode message: {e}")
            except Exception as e:
                logger.error(f"Error processing message: {e}", exc_info=True)
        
        if self._batch:
            await self._flush_batch()

    async def _flush_batch(self):
        if not self._batch:
            return
        
        batch_to_insert = self._batch
        self._batch = []
        
        try:
            inserted = self.db.insert_batch(batch_to_insert)
            logger.info(f"Persisted {inserted}/{len(batch_to_insert)} signals")
            
            if not self.config.kafka.enable_auto_commit and self.consumer:
                await self.consumer.commit()
                
        except Exception as e:
            logger.error(f"Failed to persist batch: {e}")
            self._batch = batch_to_insert + self._batch

    def _cleanup_dedup_table(self):
        try:
            self.db.cleanup_dedup_table(self.config.db.dedup_window_hours)
            self._last_cleanup = datetime.utcnow()
        except Exception as e:
            logger.error(f"Dedup cleanup failed: {e}")

    async def _shutdown(self):
        logger.info("Shutting down Persister...")
        self._running = False
        
        if self._batch:
            logger.info(f"Flushing final batch of {len(self._batch)} signals...")
            try:
                self.db.insert_batch(self._batch)
            except Exception as e:
                logger.error(f"Failed to flush final batch: {e}")
        
        if self.consumer:
            await self.consumer.stop()
        
        stats = self.db.get_stats()
        self.db.close()
        
        logger.info(f"Persister stopped. Stats: {stats.get('session_stats', {})}")

    def stop(self):
        self._running = False


async def main():
    config = Config.from_env()
    logging.getLogger().setLevel(config.app.log_level.upper())
    
    persister = Persister(config)
    
    loop = asyncio.get_event_loop()
    
    def handle_signal(sig):
        logger.info(f"Received signal {sig}, shutting down...")
        persister.stop()
    
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda s=sig: handle_signal(s))
    
    await persister.start()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        sys.exit(0)
