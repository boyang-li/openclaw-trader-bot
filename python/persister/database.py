import sqlite3
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any
from contextlib import contextmanager

from .config import DatabaseConfig

logger = logging.getLogger(__name__)


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS signals (
    id TEXT PRIMARY KEY,
    timestamp TEXT NOT NULL,
    ingested_at TEXT NOT NULL,
    processed_at TEXT,
    
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
    
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_signals_timestamp ON signals(timestamp);
CREATE INDEX IF NOT EXISTS idx_signals_source ON signals(source);
CREATE INDEX IF NOT EXISTS idx_signals_category ON signals(category);
CREATE INDEX IF NOT EXISTS idx_signals_urgency ON signals(urgency);
CREATE INDEX IF NOT EXISTS idx_signals_subject ON signals(subject);
CREATE INDEX IF NOT EXISTS idx_signals_created_at ON signals(created_at);

CREATE TABLE IF NOT EXISTS processed_ids (
    signal_id TEXT PRIMARY KEY,
    processed_at TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_processed_ids_time ON processed_ids(processed_at);
"""


class DatabaseManager:
    def __init__(self, config: DatabaseConfig):
        self.config = config
        self._connection: sqlite3.Connection | None = None
        self._stats = {"inserted": 0, "duplicates": 0, "errors": 0}

    def initialize(self):
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
        
        logger.info(f"Database initialized at {self.config.db_path}")

    def close(self):
        if self._connection:
            self._connection.close()
            self._connection = None
            logger.info("Database connection closed")

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
            (signal_id,)
        )
        return cursor.fetchone() is not None

    def mark_processed(self, signal_id: str):
        if not self._connection:
            raise RuntimeError("Database not initialized")
        self._connection.execute(
            "INSERT OR IGNORE INTO processed_ids (signal_id) VALUES (?)",
            (signal_id,)
        )

    def insert_signal(self, signal: dict[str, Any]) -> bool:
        if not self._connection:
            raise RuntimeError("Database not initialized")
        
        signal_id = signal.get("id", "")
        if not signal_id:
            logger.warning("Signal missing ID, skipping")
            self._stats["errors"] += 1
            return False
        
        if self.is_duplicate(signal_id):
            self._stats["duplicates"] += 1
            return False
        
        try:
            self._connection.execute("""
                INSERT INTO signals (
                    id, timestamp, ingested_at, processed_at,
                    source, category, subject, action, object,
                    confidence, sentiment, urgency,
                    tags, raw_data, metadata, enrichment
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                signal_id,
                signal.get("timestamp", ""),
                signal.get("metadata", {}).get("ingested_at", ""),
                signal.get("metadata", {}).get("processed_at", ""),
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
            ))
            
            self.mark_processed(signal_id)
            self._stats["inserted"] += 1
            return True
            
        except sqlite3.IntegrityError:
            self._stats["duplicates"] += 1
            return False
        except Exception as e:
            logger.error(f"Failed to insert signal {signal_id}: {e}")
            self._stats["errors"] += 1
            return False

    def insert_batch(self, signals: list[dict[str, Any]]) -> int:
        inserted = 0
        with self.transaction():
            for signal in signals:
                if self.insert_signal(signal):
                    inserted += 1
        return inserted

    def cleanup_dedup_table(self, hours: int):
        if not self._connection:
            return
        
        # Use SQLite's datetime() function for comparison since processed_at
        # is stored using datetime('now') which produces 'YYYY-MM-DD HH:MM:SS' format
        cursor = self._connection.execute(
            "DELETE FROM processed_ids WHERE processed_at < datetime('now', ?)",
            (f"-{hours} hours",)
        )
        deleted = cursor.rowcount
        self._connection.commit()
        
        if deleted > 0:
            logger.info(f"Cleaned up {deleted} old dedup records")

    def query_signals(
        self,
        source: str | None = None,
        category: str | None = None,
        urgency: str | None = None,
        subject: str | None = None,
        start_time: str | None = None,
        end_time: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        if not self._connection:
            raise RuntimeError("Database not initialized")
        
        conditions = []
        params: list[Any] = []
        
        if source:
            conditions.append("source = ?")
            params.append(source)
        if category:
            conditions.append("category = ?")
            params.append(category)
        if urgency:
            conditions.append("urgency = ?")
            params.append(urgency)
        if subject:
            conditions.append("subject LIKE ?")
            params.append(f"%{subject}%")
        if start_time:
            conditions.append("timestamp >= ?")
            params.append(start_time)
        if end_time:
            conditions.append("timestamp <= ?")
            params.append(end_time)
        
        where_clause = " WHERE " + " AND ".join(conditions) if conditions else ""
        
        params.extend([limit, offset])
        
        cursor = self._connection.execute(
            f"SELECT * FROM signals{where_clause} ORDER BY timestamp DESC LIMIT ? OFFSET ?",
            params
        )
        
        results = []
        for row in cursor.fetchall():
            signal = dict(row)
            if signal.get("tags"):
                signal["tags"] = json.loads(signal["tags"])
            if signal.get("raw_data"):
                signal["raw_data"] = json.loads(signal["raw_data"])
            if signal.get("metadata"):
                signal["metadata"] = json.loads(signal["metadata"])
            if signal.get("enrichment"):
                signal["enrichment"] = json.loads(signal["enrichment"])
            results.append(signal)
        
        return results

    def get_signal_by_id(self, signal_id: str) -> dict[str, Any] | None:
        if not self._connection:
            raise RuntimeError("Database not initialized")
        
        cursor = self._connection.execute(
            "SELECT * FROM signals WHERE id = ?",
            (signal_id,)
        )
        row = cursor.fetchone()
        
        if not row:
            return None
        
        signal = dict(row)
        if signal.get("tags"):
            signal["tags"] = json.loads(signal["tags"])
        if signal.get("raw_data"):
            signal["raw_data"] = json.loads(signal["raw_data"])
        if signal.get("metadata"):
            signal["metadata"] = json.loads(signal["metadata"])
        if signal.get("enrichment"):
            signal["enrichment"] = json.loads(signal["enrichment"])
        
        return signal

    def get_stats(self) -> dict[str, Any]:
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
        
        cursor = self._connection.execute(
            "SELECT urgency, COUNT(*) as count FROM signals GROUP BY urgency"
        )
        by_urgency = {row["urgency"]: row["count"] for row in cursor.fetchall()}
        
        return {
            "total_signals": total_signals,
            "dedup_table_size": dedup_count,
            "by_source": by_source,
            "by_urgency": by_urgency,
            "session_stats": self._stats.copy(),
        }
