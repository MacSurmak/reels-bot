from datetime import datetime
from aiogram import Router, types
from aiogram.filters import CommandStart, Command

from lexicon import t
from services.ig_client import ig_bridge
from storage import get_bridge_stats

router = Router(name="commands_router")


@router.message(CommandStart())
async def handle_start(message: types.Message) -> None:
    """Displays welcome banner and bridge instructions."""
    await message.answer(
        t("cmd_start", target_username=ig_bridge.target_username),
        parse_mode="HTML",
    )


@router.message(Command("help"))
async def handle_help(message: types.Message) -> None:
    """Displays detailed usage manual for replies and media forwarding."""
    await message.answer(t("cmd_help"), parse_mode="HTML")


@router.message(Command("status"))
async def handle_status(message: types.Message) -> None:
    """Displays real-time health status of the Instagram session and SQLite state."""
    stats = await get_bridge_stats()
    is_ok, detail = ig_bridge.check_health()

    ig_status_str = (
        t("status_ig_ok") if is_ok else t("status_ig_error", error=detail)
    )
    thread_id_str = stats["target_thread_id"] or t("status_not_initialized")
    last_item_str = stats["last_seen_item_id"] or t("status_not_initialized")

    if stats["last_sync_ts"]:
        last_sync_str = datetime.fromtimestamp(stats["last_sync_ts"]).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
    else:
        last_sync_str = t("status_not_initialized")

    await message.answer(
        t(
            "cmd_status",
            target_username=ig_bridge.target_username,
            ig_status=ig_status_str,
            thread_id=thread_id_str,
            last_item_id=last_item_str,
            last_sync_str=last_sync_str,
            total_mappings=stats["total_mappings"],
        ),
        parse_mode="HTML",
    )
