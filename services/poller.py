# ==============================================================================
# ARCHITECTURAL INVARIANT: ISOLATED SCRATCHPAD & DETERMINISTIC CURSOR COMMITS
#
# RULE: Every temporary media file downloaded from Instagram must be physically
# deleted inside a `finally` block immediately after Telegram transmission.
# Synchronization cursors must be committed to SQLite after each delivered item.
#
# FORBIDDEN:
# - Leaving downloaded .mp4, .jpg, or .ogg artifacts in TMP_MEDIA_DIR.
# - Advancing the cursor before an item is confirmed delivered to Telegram.
# ==============================================================================

import os
import html
import time
import random
import asyncio
from typing import Optional
from aiogram import Bot
from aiogram.types import FSInputFile
from loguru import logger

from config import cfg
from lexicon import t
from models import IgDirectItem, IgItemType, PreparedTelegramVideo
from services.ig_client import ig_bridge
from storage import (
    STATE_KEY_LAST_ITEM_ID,
    STATE_KEY_THREAD_ID,
    STATE_KEY_LAST_SYNC_TS,
    get_sync_state,
    set_sync_state,
    save_message_mapping,
    get_tg_id_by_ig_item_id,
)

OWNER_CHAT_ID: int = int(cfg["telegram"]["owner_id"])
POLL_INTERVAL_SECONDS: float = float(cfg["polling"]["interval_seconds"])
POLL_JITTER_SECONDS: float = float(cfg["polling"]["jitter_seconds"])

TELEGRAM_MAX_CAPTION_CHARS = 900
TELEGRAM_MAX_TEXT_CHARS = 4000


def _remove_temp_file_safely(file_path: Optional[str]) -> None:
    """Guarantees removal of temporary media artifact from disk."""
    if file_path and os.path.exists(file_path):
        try:
            os.remove(file_path)
        except OSError as err:
            logger.warning(
                f"Failed to remove temporary media file '{file_path}': {err}"
            )


def _truncate_and_escape(raw_text: Optional[str], max_chars: int) -> str:
    """Truncates raw string to safe length and escapes HTML entities."""
    if not raw_text:
        return ""
    cleaned = raw_text.strip()
    if len(cleaned) > max_chars:
        cleaned = cleaned[:max_chars] + "..."
    return html.escape(cleaned)


async def _deliver_item_to_telegram(
    bot: Bot,
    item: IgDirectItem,
    reply_to_tg_id: Optional[int],
) -> int:
    """
    Delivers a single normalized IgDirectItem to the owner's Telegram chat.
    Returns the Telegram message_id of the sent message.
    """
    sender_escaped = html.escape(ig_bridge.target_username)
    author_line = (
        t("ig_in_author_fmt", author=html.escape(item.author_username))
        if item.author_username
        else ""
    )
    escaped_caption_text = _truncate_and_escape(item.text, TELEGRAM_MAX_CAPTION_CHARS)
    text_line = (
        t("ig_in_text_fmt", text=escaped_caption_text) if escaped_caption_text else "\n"
    )

    # 1. Reel / Clip
    if item.item_type == IgItemType.REEL:
        safe_url = html.escape(item.public_url or "https://www.instagram.com/")
        caption = t(
            "ig_in_reel_caption",
            sender=sender_escaped,
            author_line=author_line,
            text_line=text_line,
            url=safe_url,
        )
        if item.media_download_url:
            prepared: Optional[PreparedTelegramVideo] = None
            try:
                prepared = await asyncio.to_thread(
                    ig_bridge.prepare_video_for_telegram,
                    item,
                )
                sent_msg = await bot.send_video(
                    chat_id=OWNER_CHAT_ID,
                    video=FSInputFile(prepared.video_path),
                    thumbnail=(
                        FSInputFile(prepared.thumb_path)
                        if prepared.thumb_path
                        else None
                    ),
                    width=prepared.width,
                    height=prepared.height,
                    duration=prepared.duration,
                    caption=caption,
                    reply_to_message_id=reply_to_tg_id,
                    supports_streaming=True,
                )
                return sent_msg.message_id
            except Exception as dl_err:
                logger.warning(
                    f"Direct video delivery failed for Reel {item.item_id} ({dl_err}). "
                    "Falling back to link card."
                )
            finally:
                if prepared is not None:
                    _remove_temp_file_safely(prepared.video_path)
                    _remove_temp_file_safely(prepared.thumb_path)

        fallback_text = t(
            "ig_in_reel_fallback_link",
            sender=sender_escaped,
            author_line=author_line,
            text_line=text_line,
            url=safe_url,
        )
        sent_msg = await bot.send_message(
            chat_id=OWNER_CHAT_ID,
            text=fallback_text,
            reply_to_message_id=reply_to_tg_id,
            disable_web_page_preview=False,
        )
        return sent_msg.message_id

    # 2. Shared Feed Post (Photo, Video, or Carousel)
    if item.item_type == IgItemType.POST:
        safe_url = html.escape(item.public_url or "https://www.instagram.com/")
        caption = t(
            "ig_in_post_caption",
            sender=sender_escaped,
            author_line=author_line,
            text_line=text_line,
            url=safe_url,
        )
        if item.media_download_url:
            is_video = ".mp4" in item.media_download_url.lower()
            if is_video:
                prepared_post_vid: Optional[PreparedTelegramVideo] = None
                try:
                    prepared_post_vid = await asyncio.to_thread(
                        ig_bridge.prepare_video_for_telegram,
                        item,
                    )
                    sent_msg = await bot.send_video(
                        chat_id=OWNER_CHAT_ID,
                        video=FSInputFile(prepared_post_vid.video_path),
                        thumbnail=(
                            FSInputFile(prepared_post_vid.thumb_path)
                            if prepared_post_vid.thumb_path
                            else None
                        ),
                        width=prepared_post_vid.width,
                        height=prepared_post_vid.height,
                        duration=prepared_post_vid.duration,
                        caption=caption,
                        reply_to_message_id=reply_to_tg_id,
                        supports_streaming=True,
                    )
                    return sent_msg.message_id
                except Exception as dl_err:
                    logger.warning(
                        f"Video download failed for post {item.item_id} ({dl_err}). Sending link card."
                    )
                finally:
                    if prepared_post_vid is not None:
                        _remove_temp_file_safely(prepared_post_vid.video_path)
                        _remove_temp_file_safely(prepared_post_vid.thumb_path)
            else:
                temp_photo_post: Optional[str] = None
                try:
                    temp_photo_post = await asyncio.to_thread(
                        ig_bridge.download_media_to_temp,
                        item.media_download_url,
                        item.item_id,
                        ".jpg",
                    )
                    sent_msg = await bot.send_photo(
                        chat_id=OWNER_CHAT_ID,
                        photo=FSInputFile(temp_photo_post),
                        caption=caption,
                        reply_to_message_id=reply_to_tg_id,
                    )
                    return sent_msg.message_id
                except Exception as dl_err:
                    logger.warning(
                        f"Photo download failed for post {item.item_id} ({dl_err}). Sending link card."
                    )
                finally:
                    _remove_temp_file_safely(temp_photo_post)

        sent_msg = await bot.send_message(
            chat_id=OWNER_CHAT_ID,
            text=caption,
            reply_to_message_id=reply_to_tg_id,
            disable_web_page_preview=False,
        )
        return sent_msg.message_id

    # 3. Direct Photo
    if item.item_type == IgItemType.PHOTO and item.media_download_url:
        temp_photo_path: Optional[str] = None
        try:
            temp_photo_path = await asyncio.to_thread(
                ig_bridge.download_media_to_temp,
                item.media_download_url,
                item.item_id,
                ".jpg",
            )
            sent_msg = await bot.send_photo(
                chat_id=OWNER_CHAT_ID,
                photo=FSInputFile(temp_photo_path),
                caption=t(
                    "ig_in_photo_caption",
                    sender=sender_escaped,
                    text_line=text_line,
                ),
                reply_to_message_id=reply_to_tg_id,
            )
            return sent_msg.message_id
        finally:
            _remove_temp_file_safely(temp_photo_path)

    # 4. Direct Video
    if item.item_type == IgItemType.VIDEO and item.media_download_url:
        prepared_vid: Optional[PreparedTelegramVideo] = None
        try:
            prepared_vid = await asyncio.to_thread(
                ig_bridge.prepare_video_for_telegram,
                item,
            )
            sent_msg = await bot.send_video(
                chat_id=OWNER_CHAT_ID,
                video=FSInputFile(prepared_vid.video_path),
                thumbnail=(
                    FSInputFile(prepared_vid.thumb_path)
                    if prepared_vid.thumb_path
                    else None
                ),
                width=prepared_vid.width,
                height=prepared_vid.height,
                duration=prepared_vid.duration,
                caption=t(
                    "ig_in_video_caption",
                    sender=sender_escaped,
                    text_line=text_line,
                ),
                reply_to_message_id=reply_to_tg_id,
                supports_streaming=True,
            )
            return sent_msg.message_id
        finally:
            if prepared_vid is not None:
                _remove_temp_file_safely(prepared_vid.video_path)
                _remove_temp_file_safely(prepared_vid.thumb_path)

    # 5. Voice Message
    if item.item_type == IgItemType.VOICE and item.media_download_url:
        temp_voice_path: Optional[str] = None
        try:
            temp_voice_path = await asyncio.to_thread(
                ig_bridge.download_media_to_temp,
                item.media_download_url,
                item.item_id,
                ".mp4",
            )
            sent_msg = await bot.send_voice(
                chat_id=OWNER_CHAT_ID,
                voice=FSInputFile(temp_voice_path),
                caption=t("ig_in_voice_caption", sender=sender_escaped),
                reply_to_message_id=reply_to_tg_id,
            )
            return sent_msg.message_id
        finally:
            _remove_temp_file_safely(temp_voice_path)

    # 6. Link Message
    if item.item_type == IgItemType.LINK:
        escaped_body = _truncate_and_escape(item.text, TELEGRAM_MAX_TEXT_CHARS)
        sent_msg = await bot.send_message(
            chat_id=OWNER_CHAT_ID,
            text=t("ig_in_link", sender=sender_escaped, text=escaped_body),
            reply_to_message_id=reply_to_tg_id,
            disable_web_page_preview=False,
        )
        return sent_msg.message_id

    # 7. Plain Text Message
    if item.item_type == IgItemType.TEXT:
        escaped_body = _truncate_and_escape(item.text, TELEGRAM_MAX_TEXT_CHARS)
        sent_msg = await bot.send_message(
            chat_id=OWNER_CHAT_ID,
            text=t("ig_in_text", sender=sender_escaped, text=escaped_body),
            reply_to_message_id=reply_to_tg_id,
        )
        return sent_msg.message_id

    # 8. Expired View-Once Raven Media
    if item.item_type == IgItemType.RAVEN_EXPIRED:
        sent_msg = await bot.send_message(
            chat_id=OWNER_CHAT_ID,
            text=t("ig_in_raven_expired", sender=sender_escaped),
            reply_to_message_id=reply_to_tg_id,
        )
        return sent_msg.message_id

    # 9. Unsupported attachment fallback
    sent_msg = await bot.send_message(
        chat_id=OWNER_CHAT_ID,
        text=t(
            "ig_in_unsupported",
            sender=sender_escaped,
            raw_type=html.escape(item.raw_item_type),
        ),
        reply_to_message_id=reply_to_tg_id,
    )
    return sent_msg.message_id


async def run_instagram_poller(bot: Bot) -> None:
    """Continuously polls Instagram Direct and forwards new items to Telegram."""
    logger.info(
        f"Instagram Direct poller started for @{ig_bridge.target_username} "
        f"(interval={POLL_INTERVAL_SECONDS}s, jitter=+{POLL_JITTER_SECONDS}s)."
    )

    while True:
        try:
            last_seen_id = await get_sync_state(STATE_KEY_LAST_ITEM_ID)
            known_thread_id = await get_sync_state(STATE_KEY_THREAD_ID)

            thread_id, newest_item_id, new_items = await asyncio.to_thread(
                ig_bridge.fetch_new_items,
                last_seen_id,
                known_thread_id,
            )

            if thread_id and thread_id != known_thread_id:
                await set_sync_state(STATE_KEY_THREAD_ID, thread_id)

            for item in new_items:
                reply_to_tg_id: Optional[int] = None
                if item.replied_to_item_id:
                    reply_to_tg_id = await get_tg_id_by_ig_item_id(
                        item.replied_to_item_id
                    )

                tg_msg_id = await _deliver_item_to_telegram(
                    bot=bot,
                    item=item,
                    reply_to_tg_id=reply_to_tg_id,
                )

                await save_message_mapping(
                    tg_message_id=tg_msg_id,
                    ig_item_id=item.item_id,
                    ig_thread_id=thread_id,
                    direction="ig_to_tg",
                    item_type=item.item_type.value,
                )

                # Commit cursor immediately after each delivered item
                await set_sync_state(STATE_KEY_LAST_ITEM_ID, item.item_id)

                logger.info(
                    f"Delivered Instagram {item.item_type.value} #{item.item_id} -> "
                    f"Telegram message #{tg_msg_id}"
                )

            if newest_item_id and newest_item_id != last_seen_id:
                await set_sync_state(STATE_KEY_LAST_ITEM_ID, newest_item_id)

            await set_sync_state(STATE_KEY_LAST_SYNC_TS, str(int(time.time())))

        except asyncio.CancelledError:
            logger.info("Instagram Direct poller task cancelled cleanly.")
            break
        except Exception as err:
            logger.error(f"[IG Poller] Error during polling cycle: {err}")

        sleep_time = POLL_INTERVAL_SECONDS + random.uniform(0.0, POLL_JITTER_SECONDS)
        await asyncio.sleep(sleep_time)
