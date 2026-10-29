import os
import asyncio
from typing import Optional
from aiogram import Router, types
from loguru import logger

from config import TMP_MEDIA_DIR
from lexicon import t
from models import IgItemType
from services.ig_client import ig_bridge
from storage import (
    STATE_KEY_THREAD_ID,
    get_ig_target_by_tg_id,
    get_sync_state,
    save_message_mapping,
)

router = Router(name="bridge_router")


def _cleanup_temp_file(path: Optional[str]) -> None:
    """Safely removes temporary uploaded Telegram media file from disk."""
    if path and os.path.exists(path):
        try:
            os.remove(path)
        except OSError as err:
            logger.warning(f"Failed to clean up temporary file '{path}': {err}")


@router.message()
async def handle_outgoing_message(message: types.Message) -> None:
    """
    Routes owner's Telegram messages (text, photo, video, video_note, animation) to Instagram Direct.
    Supports native swipe-replies and captions.
    """
    has_text = bool(message.text and message.text.strip())
    has_photo = bool(message.photo)
    video_obj = message.video or message.video_note or message.animation
    has_video = video_obj is not None

    if not (has_text or has_photo or has_video):
        await message.reply(t("tg_out_unsupported_content"), parse_mode="HTML")
        return

    reply_to_ig_item_id: Optional[str] = None
    target_thread_id: Optional[str] = None

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

    os.makedirs(TMP_MEDIA_DIR, exist_ok=True)
    temp_file_path: Optional[str] = None

    try:
        # 1. Outgoing Photo from Telegram -> Instagram Direct
        if has_photo:
            largest_photo = message.photo[-1]
            temp_file_path = os.path.join(
                TMP_MEDIA_DIR, f"tg_out_{message.message_id}.jpg"
            )
            await message.bot.download(
                largest_photo.file_id, destination=temp_file_path
            )

            sent_ig_item_id = await asyncio.to_thread(
                ig_bridge.send_direct_photo,
                target_thread_id,
                temp_file_path,
            )
            out_item_type = IgItemType.PHOTO.value

            caption_text = message.caption.strip() if message.caption else ""
            if caption_text:
                sent_ig_item_id = await asyncio.to_thread(
                    ig_bridge.send_direct_message,
                    target_thread_id,
                    caption_text,
                    reply_to_ig_item_id,
                )

        # 2. Outgoing Video / Video Note / GIF Animation from Telegram -> Instagram Direct
        elif has_video and video_obj is not None:
            temp_file_path = os.path.join(
                TMP_MEDIA_DIR, f"tg_out_{message.message_id}.mp4"
            )
            await message.bot.download(video_obj.file_id, destination=temp_file_path)

            sent_ig_item_id = await asyncio.to_thread(
                ig_bridge.send_direct_video,
                target_thread_id,
                temp_file_path,
            )
            out_item_type = IgItemType.VIDEO.value

            caption_text = message.caption.strip() if message.caption else ""
            if caption_text:
                sent_ig_item_id = await asyncio.to_thread(
                    ig_bridge.send_direct_message,
                    target_thread_id,
                    caption_text,
                    reply_to_ig_item_id,
                )

        # 3. Outgoing Text / Quote Reply from Telegram -> Instagram Direct
        else:
            text_to_send = message.text.strip()
            sent_ig_item_id = await asyncio.to_thread(
                ig_bridge.send_direct_message,
                target_thread_id,
                text_to_send,
                reply_to_ig_item_id,
            )
            out_item_type = IgItemType.TEXT.value

        await save_message_mapping(
            tg_message_id=message.message_id,
            ig_item_id=sent_ig_item_id,
            ig_thread_id=target_thread_id,
            direction="tg_to_ig",
            item_type=out_item_type,
        )

        logger.info(
            f"Forwarded Telegram {out_item_type} #{message.message_id} -> Instagram Direct "
            f"item #{sent_ig_item_id} (reply_to={reply_to_ig_item_id})"
        )

        await message.react([types.ReactionTypeEmoji(emoji=t("tg_out_reaction_ok"))])
    except Exception as err:
        logger.exception(
            f"Failed to send Telegram message #{message.message_id} to Instagram Direct: {err}"
        )
        await message.reply(
            t("tg_out_send_error", error=str(err)),
            parse_mode="HTML",
        )
    finally:
        _cleanup_temp_file(temp_file_path)
