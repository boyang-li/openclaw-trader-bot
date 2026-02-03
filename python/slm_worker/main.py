"""
SLM Worker Main - Kafka consumer loop with SLM enrichment
"""

import asyncio
import json
import logging
import signal
import sys
from datetime import datetime

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError

from .config import Config
from .processor import SLMProcessor
from .signal_schema import Signal

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("slm_worker")


class SLMWorker:
    def __init__(self, config: Config):
        self.config = config
        self.processor = SLMProcessor(config.slm)
        self.consumer: AIOKafkaConsumer | None = None
        self.producer: AIOKafkaProducer | None = None
        self._running = False
        self._processed_count = 0
        self._error_count = 0

    async def start(self):
        logger.info("Starting SLM Worker...")
        self.config.log_config()

        logger.info("Pre-loading SLM model...")
        self.processor.load_model()

        await self._connect_kafka()

        self._running = True
        logger.info("SLM Worker started, entering main loop")

        try:
            await self._consume_loop()
        finally:
            await self._shutdown()

    async def _connect_kafka(self):
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

            except KafkaConnectionError as e:
                logger.warning(f"Kafka connection failed: {e}")
                if attempt < max_retries - 1:
                    logger.info(f"Retrying in {retry_delay}s...")
                    await asyncio.sleep(retry_delay)
                else:
                    raise RuntimeError(f"Failed to connect to Kafka after {max_retries} attempts")

    async def _consume_loop(self):
        batch: list[Signal] = []
        batch_size = self.config.slm.batch_size

        async for msg in self.consumer:
            if not self._running:
                break

            try:
                raw_value = msg.value.decode("utf-8") if isinstance(msg.value, bytes) else msg.value
                signal_obj = Signal.from_json(raw_value)

                errors = signal_obj.validate()
                if errors:
                    logger.warning(f"Invalid signal {signal_obj.id}: {errors}")
                    await self._send_to_dlq(raw_value, f"Validation errors: {errors}")
                    continue

                batch.append(signal_obj)

                if len(batch) >= batch_size:
                    await self._process_batch(batch)
                    batch = []

            except json.JSONDecodeError as e:
                logger.error(f"Failed to decode message: {e}")
                raw_str = msg.value.decode("utf-8") if isinstance(msg.value, bytes) else str(msg.value)
                await self._send_to_dlq(raw_str, f"JSON decode error: {e}")
                self._error_count += 1

            except Exception as e:
                logger.error(f"Error processing message: {e}", exc_info=True)
                self._error_count += 1

        if batch:
            await self._process_batch(batch)

    async def _process_batch(self, batch: list[Signal]):
        logger.info(f"Processing batch of {len(batch)} signals")
        start = datetime.utcnow()

        for signal_obj in batch:
            try:
                enriched = self.processor.enrich_signal(signal_obj)
                await self._send_enriched(enriched)
                self._processed_count += 1

                if self._processed_count % 10 == 0:
                    logger.info(
                        f"Progress: {self._processed_count} processed, {self._error_count} errors"
                    )

            except Exception as e:
                logger.error(f"Failed to enrich signal {signal_obj.id}: {e}")
                await self._send_to_dlq(signal_obj.to_json(), f"Enrichment error: {e}")
                self._error_count += 1

        if not self.config.kafka.enable_auto_commit:
            await self.consumer.commit()

        elapsed = (datetime.utcnow() - start).total_seconds()
        logger.info(f"Batch processed in {elapsed:.2f}s")

    async def _send_enriched(self, signal_obj: Signal):
        await self.producer.send_and_wait(
            self.config.kafka.output_topic,
            key=signal_obj.kafka_key(),
            value=signal_obj.to_json(),
        )

    async def _send_to_dlq(self, raw_message: str, error: str):
        dlq_envelope = {
            "original_message": raw_message,
            "error": error,
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "worker_id": self.config.app.worker_id,
        }
        await self.producer.send_and_wait(
            self.config.kafka.dlq_topic,
            value=json.dumps(dlq_envelope),
        )

    async def _shutdown(self):
        logger.info("Shutting down SLM Worker...")
        self._running = False

        if self.consumer:
            await self.consumer.stop()
        if self.producer:
            await self.producer.stop()

        logger.info(
            f"SLM Worker stopped. Total processed: {self._processed_count}, errors: {self._error_count}"
        )

    def stop(self):
        self._running = False


async def main():
    config = Config.from_env()
    logging.getLogger().setLevel(config.app.log_level.upper())

    worker = SLMWorker(config)

    loop = asyncio.get_event_loop()

    def handle_signal(sig):
        logger.info(f"Received signal {sig}, shutting down...")
        worker.stop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda s=sig: handle_signal(s))

    await worker.start()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        sys.exit(0)
