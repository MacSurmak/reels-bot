# ==============================================================================
# ARCHITECTURAL INVARIANT: MODULE LOAD ORDER & TOP-LEVEL IMPORT DISCIPLINE
#
# RULE: All module dependencies MUST be explicitly declared at the top of the file
# according to PEP 8 standards. Fail-Fast on boot: if any dependency or symbol
# is missing, the application must crash loudly during container initialization.
#
# FORBIDDEN:
# - Placing `import` statements inside functions or coroutines (lazy imports).
# - Swallowing initialization errors to mask missing infrastructure.
# ==============================================================================

import os
import asyncio
from contextlib import suppress
from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from loguru import logger

from config import (
    cfg,
    TMP_MEDIA_DIR,
    GC_INTERVAL_SECONDS,
    MAPPING_RETENTION_TTL_SECONDS,
)
from storage import init_db, close_db, cleanup_expired_mappings
from middlewares import OwnerAuthMiddleware
from handlers import main_router
from lexicon import set_bot_commands
from services import setup_logger
from services.ig_client import ig_bridge
from services.poller import run_instagram_poller


def sanitize_temp_workspace() -> None:
    """Purges any residual files inside TMP_MEDIA_DIR on startup."""
    if os.path.exists(TMP_MEDIA_DIR):
        for fname in os.listdir(TMP_MEDIA_DIR):
            fpath = os.path.join(TMP_MEDIA_DIR, fname)
            try:
                if os.path.isfile(fpath):
                    os.remove(fpath)
            except OSError as err:
                logger.warning(
                    f"Could not remove residual file '{fpath}' during startup sanitization: {err}"
                )
    os.makedirs(TMP_MEDIA_DIR, exist_ok=True)
    logger.info(f"Temporary media workspace '{TMP_MEDIA_DIR}' sanitized.")


async def run_garbage_collector() -> None:
    """Periodically purges expired message mappings from SQLite."""
    while True:
        try:
            await asyncio.sleep(GC_INTERVAL_SECONDS)
            purged_count = await cleanup_expired_mappings(MAPPING_RETENTION_TTL_SECONDS)
            if purged_count > 0:
                logger.info(
                    f"[GC] Purged {purged_count} expired message mapping(s) from SQLite."
                )
        except asyncio.CancelledError:
            break
        except Exception as err:
            logger.error(f"[GC] Error in garbage collector task: {err}")


async def main() -> None:
    """Initializes infrastructure, verifies Instagram session, and starts the bridge."""
    setup_logger("INFO")
    sanitize_temp_workspace()

    logger.info("Initializing SQLite persistence layer...")
    await init_db()

    logger.info("Authenticating Instagram Direct session...")
    await asyncio.to_thread(ig_bridge.authenticate)

    bot_token = cfg["telegram"]["bot_token"].strip()
    bot = Bot(token=bot_token, default=DefaultBotProperties(parse_mode="HTML"))
    dp = Dispatcher()

    auth_guard = OwnerAuthMiddleware()
    dp.message.middleware(auth_guard)
    dp.include_router(main_router)

    await set_bot_commands(bot)
    await bot.delete_webhook(drop_pending_updates=True)

    poller_task = asyncio.create_task(run_instagram_poller(bot))
    gc_task = asyncio.create_task(run_garbage_collector())

    logger.info("Starting Telegram Bot long-polling...")
    try:
        await dp.start_polling(bot)
    finally:
        logger.info("Shutting down background tasks and closing connections...")
        poller_task.cancel()
        gc_task.cancel()
        with suppress(asyncio.CancelledError):
            await asyncio.gather(poller_task, gc_task)
        await bot.session.close()
        await close_db()
        logger.info("Bridge shutdown complete.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Bot stopped manually.")
