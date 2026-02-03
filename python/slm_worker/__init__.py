"""
ACC L1-Ingestion: SLM Worker
============================
Consumes raw signals from Kafka, enriches them using a local SLM (Qwen 2.5-1.5B),
and publishes enriched signals back to Kafka.
"""

__version__ = "0.1.0"
