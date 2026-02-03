#!/bin/bash
set -euo pipefail

BACKUP_DIR="${BACKUP_DIR:-./backups}"
CONTAINER_NAME="${CONTAINER_NAME:-l1-persister}"
DB_PATH="${DB_PATH:-/data/signals.db}"
RETENTION_DAYS="${RETENTION_DAYS:-7}"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

mkdir -p "$BACKUP_DIR"

echo "=== L1-Ingestion SQLite Backup ==="
echo "Timestamp: $TIMESTAMP"
echo "Container: $CONTAINER_NAME"
echo "Backup dir: $BACKUP_DIR"

BACKUP_FILE="$BACKUP_DIR/signals_${TIMESTAMP}.db"

if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
    echo "Error: Container $CONTAINER_NAME is not running"
    exit 1
fi

echo "Creating backup checkpoint..."
docker exec "$CONTAINER_NAME" sqlite3 "$DB_PATH" "PRAGMA wal_checkpoint(TRUNCATE);" || true

echo "Copying database..."
docker cp "${CONTAINER_NAME}:${DB_PATH}" "$BACKUP_FILE"

if [ -f "$BACKUP_FILE" ]; then
    SIZE=$(du -h "$BACKUP_FILE" | cut -f1)
    echo "Backup created: $BACKUP_FILE ($SIZE)"
    
    COMPRESSED_FILE="${BACKUP_FILE}.gz"
    echo "Compressing backup..."
    gzip -c "$BACKUP_FILE" > "$COMPRESSED_FILE"
    rm "$BACKUP_FILE"
    COMPRESSED_SIZE=$(du -h "$COMPRESSED_FILE" | cut -f1)
    echo "Compressed: $COMPRESSED_FILE ($COMPRESSED_SIZE)"
else
    echo "Error: Backup file not created"
    exit 1
fi

echo "Cleaning up old backups (older than $RETENTION_DAYS days)..."
find "$BACKUP_DIR" -name "signals_*.db.gz" -mtime +"$RETENTION_DAYS" -delete 2>/dev/null || true

BACKUP_COUNT=$(ls -1 "$BACKUP_DIR"/signals_*.db.gz 2>/dev/null | wc -l || echo "0")
echo "Total backups: $BACKUP_COUNT"

echo "=== Backup Complete ==="
