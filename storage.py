# ==============================================================================
# ARCHITECTURAL INVARIANT: PERSISTENCE LAYER STRICT VALIDATION & SSOT
#
# RULE: All synchronization cursors and message ID mappings must be stored
# transactionally in SQLite. Storage functions must validate input invariants
# before executing SQL statements.
#
# FORBIDDEN:
# - Storing synchronization cursors only in volatile RAM.
# - Coercing invalid or empty IDs into dummy values.
# ==============================================================================

import os
import time
from typing import Optional, Tuple, Dict, Any
import aiosqlite
from loguru import logger

from config import DATA_DIR, DB_PATH

_DB_CONN: Optional[aiosqlite.Connection] = None

VALID_DIRECTIONS = ("ig_to_tg", "tg_to_ig")
STATE_KEY_LAST_ITEM_ID = "last_seen_item_id"
STATE_KEY_THREAD_ID = "target_thread_id"
STATE_KEY_LAST_SYNC_TS = "last_sync_timestamp"


def get_db() -> aiosqlite.Connection:
    """Returns active SQLite connection with fail-fast initialization check."""
    if _DB_CONN is None:
        raise RuntimeError(
            "Database connection is not initialized. Ensure 'await init_db()' was called on startup."
        )
    return _DB_CONN


async def init_db() -> aiosqlite.Connection:
    """Initializes SQLite connection in WAL mode and applies idempotent schema migrations."""
    global _DB_CONN

    os.makedirs(DATA_DIR, exist_ok=True)
    logger.info(f"Opening SQLite database at '{DB_PATH}'...")

    _DB_CONN = await aiosqlite.connect(DB_PATH)
    _DB_CONN.row_factory = aiosqlite.Row

    await _DB_CONN.execute("PRAGMA journal_mode=WAL;")
    await _DB_CONN.execute("PRAGMA synchronous=NORMAL;")
    await _DB_CONN.execute("PRAGMA foreign_keys=ON;")

    await _DB_CONN.execute(
        """
        CREATE TABLE IF NOT EXISTS message_mappings (
            tg_message_id INTEGER PRIMARY KEY,
            ig_item_id TEXT NOT NULL UNIQUE,
            ig_thread_id TEXT NOT NULL,
            direction TEXT NOT NULL,
            item_type TEXT NOT NULL,
            created_at INTEGER NOT NULL
        );
        """
    )
    await _DB_CONN.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_mappings_ig_item
        ON message_mappings (ig_item_id);
        """
    )
    await _DB_CONN.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_mappings_created_at
        ON message_mappings (created_at);
        """
    )

    await _DB_CONN.execute(
        """
        CREATE TABLE IF NOT EXISTS sync_state (
            state_key TEXT PRIMARY KEY,
            state_value TEXT NOT NULL,
            updated_at INTEGER NOT NULL
        );
        """
    )

    await _DB_CONN.commit()
    logger.info("SQLite WAL schema initialized and verified successfully.")
    return _DB_CONN


async def close_db() -> None:
    """Gracefully closes SQLite database connection."""
    global _DB_CONN
    if _DB_CONN is not None:
        await _DB_CONN.close()
        _DB_CONN = None
        logger.info("SQLite database connection closed.")


async def save_message_mapping(
    tg_message_id: int,
    ig_item_id: str,
    ig_thread_id: str,
    direction: str,
    item_type: str,
) -> None:
    """Persists bidirectional link between a Telegram message ID and an Instagram Direct item ID."""
    if not isinstance(tg_message_id, int) or tg_message_id <= 0:
        raise ValueError(f"Invalid tg_message_id: {tg_message_id}")
    if not ig_item_id or not ig_item_id.strip():
        raise ValueError("Parameter 'ig_item_id' cannot be empty.")
    if not ig_thread_id or not ig_thread_id.strip():
        raise ValueError("Parameter 'ig_thread_id' cannot be empty.")
    if direction not in VALID_DIRECTIONS:
        raise ValueError(
            f"Invalid direction '{direction}'. Must be one of {VALID_DIRECTIONS}."
        )
    if not item_type or not item_type.strip():
        raise ValueError("Parameter 'item_type' cannot be empty.")

    db = get_db()
    now_ts = int(time.time())
    await db.execute(
        """
        INSERT INTO message_mappings (
            tg_message_id, ig_item_id, ig_thread_id, direction, item_type, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(tg_message_id) DO UPDATE SET
            ig_item_id = excluded.ig_item_id,
            ig_thread_id = excluded.ig_thread_id,
            direction = excluded.direction,
            item_type = excluded.item_type,
            created_at = excluded.created_at;
        """,
        (
            tg_message_id,
            ig_item_id.strip(),
            ig_thread_id.strip(),
            direction,
            item_type.strip(),
            now_ts,
        ),
    )
    await db.commit()


async def get_ig_target_by_tg_id(tg_message_id: int) -> Optional[Tuple[str, str]]:
    """Resolves (ig_item_id, ig_thread_id) for a given Telegram message ID."""
    if not isinstance(tg_message_id, int) or tg_message_id <= 0:
        raise ValueError(f"Invalid tg_message_id: {tg_message_id}")

    db = get_db()
    async with db.execute(
        "SELECT ig_item_id, ig_thread_id FROM message_mappings WHERE tg_message_id = ?;",
        (tg_message_id,),
    ) as cursor:
        row = await cursor.fetchone()
        if not row:
            return None
        return (row["ig_item_id"], row["ig_thread_id"])


async def get_tg_id_by_ig_item_id(ig_item_id: str) -> Optional[int]:
    """Resolves Telegram message ID for a given Instagram item ID to preserve reply threads."""
    if not ig_item_id or not ig_item_id.strip():
        raise ValueError("Parameter 'ig_item_id' cannot be empty.")

    db = get_db()
    async with db.execute(
        "SELECT tg_message_id FROM message_mappings WHERE ig_item_id = ?;",
        (ig_item_id.strip(),),
    ) as cursor:
        row = await cursor.fetchone()
        if not row:
            return None
        return int(row["tg_message_id"])


async def set_sync_state(key: str, value: str) -> None:
    """Atomically updates a persistent synchronization state key."""
    if not key or not key.strip():
        raise ValueError("Sync state key cannot be empty.")
    if value is None or not str(value).strip():
        raise ValueError(f"Sync state value for key '{key}' cannot be empty.")

    db = get_db()
    now_ts = int(time.time())
    await db.execute(
        """
        INSERT INTO sync_state (state_key, state_value, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(state_key) DO UPDATE SET
            state_value = excluded.state_value,
            updated_at = excluded.updated_at;
        """,
        (key.strip(), str(value).strip(), now_ts),
    )
    await db.commit()


async def get_sync_state(key: str) -> Optional[str]:
    """Retrieves a persistent synchronization state value by key."""
    if not key or not key.strip():
        raise ValueError("Sync state key cannot be empty.")

    db = get_db()
    async with db.execute(
        "SELECT state_value FROM sync_state WHERE state_key = ?;",
        (key.strip(),),
    ) as cursor:
        row = await cursor.fetchone()
        if not row:
            return None
        return str(row["state_value"])


async def get_bridge_stats() -> Dict[str, Any]:
    """Returns diagnostic metrics for the /status command."""
    db = get_db()
    async with db.execute("SELECT COUNT(*) AS cnt FROM message_mappings;") as cur:
        row = await cur.fetchone()
        total_mappings = int(row["cnt"]) if row else 0

    last_item_id = await get_sync_state(STATE_KEY_LAST_ITEM_ID)
    thread_id = await get_sync_state(STATE_KEY_THREAD_ID)
    last_sync_raw = await get_sync_state(STATE_KEY_LAST_SYNC_TS)

    return {
        "total_mappings": total_mappings,
        "last_seen_item_id": last_item_id,
        "target_thread_id": thread_id,
        "last_sync_ts": int(last_sync_raw) if last_sync_raw else None,
    }


async def cleanup_expired_mappings(ttl_seconds: int) -> int:
    """Purges message mappings older than ttl_seconds and returns deleted row count."""
    if not isinstance(ttl_seconds, int) or ttl_seconds <= 0:
        raise ValueError(f"Invalid ttl_seconds: {ttl_seconds}")

    db = get_db()
    cutoff_ts = int(time.time()) - ttl_seconds
    cursor = await db.execute(
        "DELETE FROM message_mappings WHERE created_at < ?;",
        (cutoff_ts,),
    )
    await db.commit()
    return cursor.rowcount
