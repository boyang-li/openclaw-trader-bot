"""SQLite persistence for L2 Reasoner."""

from __future__ import annotations

import json
import logging
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import cast

from ..config import DatabaseConfig

logger = logging.getLogger(__name__)


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS signals (
    id TEXT PRIMARY KEY,
    timestamp TEXT NOT NULL,
    source TEXT NOT NULL,
    category TEXT NOT NULL,
    subject TEXT NOT NULL,
    action TEXT NOT NULL,
    object TEXT,
    confidence REAL,
    sentiment REAL,
    urgency TEXT,
    tags TEXT,
    raw_data TEXT,
    metadata TEXT,
    enrichment TEXT,
    entities TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_l2_signals_timestamp ON signals(timestamp);
CREATE INDEX IF NOT EXISTS idx_l2_signals_source ON signals(source);
CREATE INDEX IF NOT EXISTS idx_l2_signals_category ON signals(category);
CREATE INDEX IF NOT EXISTS idx_l2_signals_subject ON signals(subject);
CREATE INDEX IF NOT EXISTS idx_l2_signals_created_at ON signals(created_at);

CREATE TABLE IF NOT EXISTS processed_ids (
    signal_id TEXT PRIMARY KEY,
    processed_at TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_l2_processed_ids_time ON processed_ids(processed_at);
"""


class DatabaseManager:
    def __init__(self, config: DatabaseConfig):
        self.config = config
        self._connection: sqlite3.Connection | None = None
        self._stats = {"inserted": 0, "duplicates": 0, "errors": 0}

    def initialize(self) -> None:
        db_path = Path(self.config.db_path)
        db_path.parent.mkdir(parents=True, exist_ok=True)

        self._connection = sqlite3.connect(
            self.config.db_path,
            timeout=self.config.busy_timeout_ms / 1000.0,
            check_same_thread=False,
        )
        self._connection.row_factory = sqlite3.Row

        self._connection.execute(f"PRAGMA journal_mode={self.config.journal_mode}")
        self._connection.execute(f"PRAGMA busy_timeout={self.config.busy_timeout_ms}")
        self._connection.execute(f"PRAGMA cache_size={self.config.cache_size_pages}")
        self._connection.execute("PRAGMA synchronous=NORMAL")

        self._connection.executescript(SCHEMA_SQL)
        self._connection.commit()

        if self.config.vacuum_on_startup:
            logger.info("Running VACUUM on startup...")
            self._connection.execute("VACUUM")

        logger.info(f"L2 database initialized at {self.config.db_path}")

    def close(self) -> None:
        if self._connection:
            self._connection.close()
            self._connection = None
            logger.info("L2 database connection closed")

    @contextmanager
    def transaction(self):
        if not self._connection:
            raise RuntimeError("Database not initialized")
        try:
            yield self._connection
            self._connection.commit()
        except Exception:
            self._connection.rollback()
            raise

    def is_duplicate(self, signal_id: str) -> bool:
        if not self._connection:
            raise RuntimeError("Database not initialized")
        cursor = self._connection.execute(
            "SELECT 1 FROM processed_ids WHERE signal_id = ?",
            (signal_id,),
        )
        return cursor.fetchone() is not None

    def mark_processed(self, signal_id: str) -> None:
        if not self._connection:
            raise RuntimeError("Database not initialized")
        self._connection.execute(
            "INSERT OR IGNORE INTO processed_ids (signal_id) VALUES (?)",
            (signal_id,),
        )

    def insert_signal(self, signal: dict[str, object]) -> bool:
        if not self._connection:
            raise RuntimeError("Database not initialized")

        signal_id = str(signal.get("id", ""))
        if not signal_id:
            logger.warning("Signal missing ID, skipping")
            self._stats["errors"] += 1
            return False

        if self.is_duplicate(signal_id):
            self._stats["duplicates"] += 1
            return False

        try:
            self._connection.execute(
                """
                INSERT INTO signals (
                    id, timestamp, source, category, subject, action, object,
                    confidence, sentiment, urgency, tags, raw_data, metadata, enrichment, entities
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    signal_id,
                    signal.get("timestamp", ""),
                    signal.get("source", ""),
                    signal.get("category", ""),
                    signal.get("subject", ""),
                    signal.get("action", ""),
                    signal.get("object"),
                    signal.get("confidence", 0.0),
                    signal.get("sentiment", 0.0),
                    signal.get("urgency", "low"),
                    json.dumps(signal.get("tags", [])),
                    json.dumps(signal.get("raw_data")) if signal.get("raw_data") else None,
                    json.dumps(signal.get("metadata", {})),
                    json.dumps(signal.get("enrichment")) if signal.get("enrichment") else None,
                    json.dumps(signal.get("entities", [])),
                ),
            )

            self.mark_processed(signal_id)
            self._stats["inserted"] += 1
            return True
        except sqlite3.IntegrityError:
            self._stats["duplicates"] += 1
            return False
        except Exception as exc:
            logger.error(f"Failed to insert signal {signal_id}: {exc}")
            self._stats["errors"] += 1
            return False

    def insert_batch(self, signals: list[dict[str, object]]) -> int:
        inserted = 0
        with self.transaction():
            for signal in signals:
                if self.insert_signal(signal):
                    inserted += 1
        return inserted

    def load_recent_signals(self, window_hours: int) -> list[dict[str, object]]:
        if not self._connection:
            raise RuntimeError("Database not initialized")

        cutoff = (
            datetime.now(timezone.utc) - timedelta(hours=window_hours)
        ).isoformat().replace("+00:00", "Z")
        cursor = self._connection.execute(
            "SELECT * FROM signals WHERE timestamp >= ? ORDER BY timestamp ASC",
            (cutoff,),
        )
        results: list[dict[str, object]] = []
        for row in cursor.fetchall():
            signal = dict(row)
            if signal.get("tags"):
                signal["tags"] = cast(list[str], json.loads(signal["tags"]))
            if signal.get("raw_data"):
                signal["raw_data"] = cast(dict[str, object], json.loads(signal["raw_data"]))
            if signal.get("metadata"):
                signal["metadata"] = cast(dict[str, object], json.loads(signal["metadata"]))
            if signal.get("enrichment"):
                signal["enrichment"] = cast(dict[str, object], json.loads(signal["enrichment"]))
            if signal.get("entities"):
                signal["entities"] = cast(list[str], json.loads(signal["entities"]))
            results.append(signal)
        return results

    def cleanup_dedup_table(self, hours: int) -> None:
        if not self._connection:
            return
        cursor = self._connection.execute(
            "DELETE FROM processed_ids WHERE processed_at < datetime('now', ?)",
            (f"-{hours} hours",),
        )
        deleted = cursor.rowcount
        self._connection.commit()
        if deleted > 0:
            logger.info(f"Cleaned up {deleted} old dedup records")

    def get_stats(self) -> dict[str, object]:
        if not self._connection:
            return {"status": "not_initialized"}

        cursor = self._connection.execute("SELECT COUNT(*) FROM signals")
        total_signals = cursor.fetchone()[0]

        cursor = self._connection.execute("SELECT COUNT(*) FROM processed_ids")
        dedup_count = cursor.fetchone()[0]

        cursor = self._connection.execute(
            "SELECT source, COUNT(*) as count FROM signals GROUP BY source"
        )
        by_source = {row["source"]: row["count"] for row in cursor.fetchall()}

        return {
            "total_signals": total_signals,
            "dedup_table_size": dedup_count,
            "by_source": by_source,
            "session_stats": self._stats.copy(),
        }
