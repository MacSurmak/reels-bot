from typing import Any, Awaitable, Callable, Dict
from aiogram import BaseMiddleware
from aiogram.types import TelegramObject, Message
from loguru import logger

from config import cfg
from lexicon import t

OWNER_TELEGRAM_ID: int = int(cfg["telegram"]["owner_id"])


class OwnerAuthMiddleware(BaseMiddleware):
    """Enforces strict single-owner access control across all incoming Telegram messages."""

    async def __call__(
        self,
        handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: Dict[str, Any],
    ) -> Any:
        user = data.get("event_from_user")
        if not user or user.is_bot:
            return None

        if user.id == OWNER_TELEGRAM_ID:
            return await handler(event, data)

        logger.warning(
            f"Unauthorized access blocked: user_id={user.id}, "
            f"username=@{user.username}, full_name='{user.full_name}'"
        )

        if isinstance(event, Message):
            await event.answer(t("auth_access_denied"), parse_mode="HTML")

        return None
