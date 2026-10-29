import asyncio
from aiogram import Router, types
from loguru import logger

from lexicon import t
from models import IgItemType
from services import ig_bridge
from storage import (
    STATE_KEY_THREAD_ID,
    get_ig_target_by_tg_id,
    get_sync_state,
    save_message_mapping,
)

router = Router(name="bridge_router")


@router.message()
async def handle_outgoing_message(message: types.Message) -> None:
    """
    Routes owner's Telegram messages to Instagram Direct.
    - If message is a Reply: resolves original Instagram item_id and sends a native swipe-reply.
    - If message is standalone text: sends directly into the active thread with target_username.
    """
    if not message.text or not message.text.strip():
        await message.reply(t("tg_out_unsupported_content"), parse_mode="HTML")
        return

    text_to_send = message.text.strip()
    reply_to_ig_item_id = None
    target_thread_id = None

    if message.reply_to_message:
        replied_tg_id = message.reply_to_message.message_id
        mapping = await get_ig_target_by_tg_id(replied_tg_id)
        if not mapping:
            await message.reply(t("tg_out_reply_not_found"), parse_mode="HTML")
            return
        reply_to_ig_item_id, target_thread_id = mapping
    else:
        target_thread_id = await get_sync_state(STATE_KEY_THREAD_ID)
        if not target_thread_id:
            await message.reply(
                t("tg_out_no_thread_yet", target_username=ig_bridge.target_username),
                parse_mode="HTML",
            )
            return

    try:
        sent_ig_item_id = await asyncio.to_thread(
            ig_bridge.send_direct_message,
            target_thread_id,
            text_to_send,
            reply_to_ig_item_id,
        )

        await save_message_mapping(
            tg_message_id=message.message_id,
            ig_item_id=sent_ig_item_id,
            ig_thread_id=target_thread_id,
            direction="tg_to_ig",
            item_type=IgItemType.TEXT.value,
        )

        logger.info(
            f"Forwarded Telegram message #{message.message_id} -> Instagram Direct "
            f"item #{sent_ig_item_id} (reply_to={reply_to_ig_item_id})"
        )

        await message.react(
            [types.ReactionTypeEmoji(emoji=t("tg_out_reaction_ok"))]
        )
    except Exception as err:
        logger.exception(
            f"Failed to send Telegram message #{message.message_id} to Instagram Direct: {err}"
        )
        await message.reply(
            t("tg_out_send_error", error=str(err)),
            parse_mode="HTML",
        )
