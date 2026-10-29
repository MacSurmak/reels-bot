# ==============================================================================
# ARCHITECTURAL INVARIANT: FAIL-FAST CONFIGURATION & ZERO MAGIC NUMBERS
#
# RULE: Every required configuration parameter must be validated strictly during
# module initialization before any network or database operations occur.
# All algorithmic thresholds, timeouts, and buffer sizes must be declared here.
#
# FORBIDDEN:
# - Providing silent fallback defaults for missing credentials or IDs.
# - Embedding inline numeric literals in SQL queries, pollers, or HTTP calls.
# ==============================================================================

import os
from typing import Dict, Any
import yaml

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, "config.yaml")

DATA_DIR = os.path.join(BASE_DIR, "data")
TMP_MEDIA_DIR = os.path.join(DATA_DIR, "tmp_media")
DB_PATH = os.path.join(DATA_DIR, "bridge.db")
IG_SESSION_PATH = os.path.join(DATA_DIR, "ig_session.json")

# System lifecycle and limits constants
MAPPING_RETENTION_TTL_SECONDS = 30 * 24 * 3600  # 30 days
GC_INTERVAL_SECONDS = 3600  # 1 hour
MEDIA_DOWNLOAD_TIMEOUT_SECONDS = 30
MEDIA_DOWNLOAD_CHUNK_SIZE = 65536
TELEGRAM_MAX_VIDEO_BYTES = 50 * 1024 * 1024  # 50 MB Telegram Bot API limit
IG_REQUEST_DELAY_MIN_SECONDS = 1.0
IG_REQUEST_DELAY_MAX_SECONDS = 3.0
MIN_POLL_INTERVAL_SECONDS = 5.0
MAX_BATCH_FETCH_LIMIT = 50


class ConfigurationError(Exception):
    """Raised when config.yaml is missing, malformed, or violates domain constraints."""


def load_and_validate_config() -> Dict[str, Any]:
    """Loads and validates config.yaml with strict fail-fast semantics."""
    if not os.path.exists(CONFIG_PATH):
        raise ConfigurationError(
            f"CRITICAL CONFIGURATION ERROR: Configuration file not found at '{CONFIG_PATH}'. "
            "Copy 'config.yaml.example' to 'config.yaml' and populate required fields."
        )

    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    if not isinstance(config, dict):
        raise ConfigurationError("Configuration file must contain a valid YAML mapping.")

    # 1. Validate Telegram section
    telegram = config.get("telegram")
    if not isinstance(telegram, dict):
        raise ConfigurationError("Missing or invalid 'telegram' section in config.yaml.")

    bot_token = telegram.get("bot_token")
    if not isinstance(bot_token, str) or not bot_token.strip():
        raise ConfigurationError("Missing or empty 'telegram.bot_token' in config.yaml.")

    owner_id = telegram.get("owner_id")
    if not isinstance(owner_id, int) or isinstance(owner_id, bool) or owner_id <= 0:
        raise ConfigurationError(
            "Parameter 'telegram.owner_id' must be a positive integer Telegram user ID."
        )

    # 2. Validate Instagram section
    instagram = config.get("instagram")
    if not isinstance(instagram, dict):
        raise ConfigurationError("Missing or invalid 'instagram' section in config.yaml.")

    for field in ("username", "password", "target_username"):
        val = instagram.get(field)
        if not isinstance(val, str) or not val.strip():
            raise ConfigurationError(
                f"Missing or empty required Instagram parameter: 'instagram.{field}'."
            )

    # 3. Validate Polling section
    polling = config.get("polling")
    if not isinstance(polling, dict):
        raise ConfigurationError("Missing or invalid 'polling' section in config.yaml.")

    interval = polling.get("interval_seconds")
    if not isinstance(interval, (int, float)) or float(interval) < MIN_POLL_INTERVAL_SECONDS:
        raise ConfigurationError(
            f"Parameter 'polling.interval_seconds' must be a number >= {MIN_POLL_INTERVAL_SECONDS}."
        )

    jitter = polling.get("jitter_seconds")
    if not isinstance(jitter, (int, float)) or float(jitter) < 0:
        raise ConfigurationError(
            "Parameter 'polling.jitter_seconds' must be a non-negative number."
        )

    batch_limit = polling.get("batch_fetch_limit")
    if (
        not isinstance(batch_limit, int)
        or isinstance(batch_limit, bool)
        or not (1 <= batch_limit <= MAX_BATCH_FETCH_LIMIT)
    ):
        raise ConfigurationError(
            f"Parameter 'polling.batch_fetch_limit' must be an integer between 1 and {MAX_BATCH_FETCH_LIMIT}."
        )

    return config


cfg = load_and_validate_config()
