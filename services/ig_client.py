# ==============================================================================
# ARCHITECTURAL INVARIANT: ISOLATED EXTERNAL GATEWAY & DETERMINISTIC PARSING
#
# RULE: All interactions with the Instagram Private API are encapsulated within
# this module, protected by a thread lock for safe execution inside asyncio
# thread pools, and normalized into immutable domain DTOs (IgDirectItem).
#
# FORBIDDEN:
# - Leaking raw instagrapi objects into Telegram handlers.
# - Leaving partial or orphaned media downloads inside TMP_MEDIA_DIR on error.
# ==============================================================================

import os
import re
import threading
from collections import OrderedDict
from types import SimpleNamespace
from typing import Optional, List, Tuple, Any
import requests
from instagrapi import Client
from loguru import logger

from config import (
    cfg,
    DATA_DIR,
    TMP_MEDIA_DIR,
    IG_SESSION_PATH,
    IG_REQUEST_DELAY_MIN_SECONDS,
    IG_REQUEST_DELAY_MAX_SECONDS,
    MEDIA_DOWNLOAD_TIMEOUT_SECONDS,
    MEDIA_DOWNLOAD_CHUNK_SIZE,
    TELEGRAM_MAX_VIDEO_BYTES,
)
from models import IgDirectItem, IgItemType
from services.logger import setup_logger

CLIENT_CONTEXT_CACHE_SIZE = 500
REEL_CODE_REGEX = re.compile(r"instagram\.com/(?:reel|reels|p)/([A-Za-z0-9_-]+)")


class InstagramBridgeError(Exception):
    """Raised when an Instagram Direct operation, authentication, or media download fails."""


class InstagramBridgeClient:
    """Thread-safe Instagram Direct gateway managing session state, polling, and replies."""

    def __init__(self) -> None:
        ig_cfg = cfg["instagram"]
        self.username: str = ig_cfg["username"].strip()
        self.password: str = ig_cfg["password"].strip()
        self.target_username: str = ig_cfg["target_username"].strip().lstrip("@")
        self.device_profile: dict = cfg["device"]
        self.batch_fetch_limit: int = int(cfg["polling"]["batch_fetch_limit"])

        self._lock = threading.Lock()
        self._client = Client()
        self._client.delay_range = [
            IG_REQUEST_DELAY_MIN_SECONDS,
            IG_REQUEST_DELAY_MAX_SECONDS,
        ]
        self._target_user_id: Optional[str] = None
        self._is_authenticated: bool = False
        self._client_context_cache: OrderedDict[str, str] = OrderedDict()

    def _cache_client_context(
        self, item_id: str, client_context: Optional[str]
    ) -> None:
        """Stores item client_context in bounded FIFO cache for swipe-reply correlation."""
        if not item_id or not client_context:
            return
        self._client_context_cache[item_id] = str(client_context)
        self._client_context_cache.move_to_end(item_id)
        while len(self._client_context_cache) > CLIENT_CONTEXT_CACHE_SIZE:
            self._client_context_cache.popitem(last=False)

    def authenticate(self) -> None:
        """Authenticates with Instagram using persisted session settings or fresh login."""
        with self._lock:
            os.makedirs(DATA_DIR, exist_ok=True)
            self._client.set_device(self.device_profile)

            if os.path.exists(IG_SESSION_PATH):
                logger.info(
                    f"Loading persisted Instagram session from '{IG_SESSION_PATH}'..."
                )
                try:
                    self._client.load_settings(IG_SESSION_PATH)
                    self._client.login(self.username, self.password)
                    self._client.get_timeline_feed()
                    self._client.dump_settings(IG_SESSION_PATH)
                    self._is_authenticated = True
                    logger.info(
                        f"Instagram session verified and refreshed for @{self.username}."
                    )
                except Exception as err:
                    logger.warning(
                        f"Stored session validation failed ({err}). Performing fresh authentication..."
                    )
                    self._perform_fresh_login()
            else:
                logger.info(
                    f"No existing session file found at '{IG_SESSION_PATH}'. Performing fresh login..."
                )
                self._perform_fresh_login()

            self._resolve_target_user_id_locked()

    def _perform_fresh_login(self) -> None:
        """Executes fresh credential login and persists session settings to disk."""
        try:
            self._client = Client()
            self._client.delay_range = [
                IG_REQUEST_DELAY_MIN_SECONDS,
                IG_REQUEST_DELAY_MAX_SECONDS,
            ]
            self._client.set_device(self.device_profile)
            self._client.login(self.username, self.password)
            self._client.dump_settings(IG_SESSION_PATH)
            self._is_authenticated = True
            logger.info(
                f"Fresh login succeeded for @{self.username}. Session saved to '{IG_SESSION_PATH}'."
            )
        except Exception as err:
            self._is_authenticated = False
            raise InstagramBridgeError(
                f"Failed to authenticate Instagram user @{self.username}: {err}"
            ) from err

    def _resolve_target_user_id_locked(self) -> str:
        """Resolves and caches numerical Instagram PK for target_username."""
        if self._target_user_id:
            return self._target_user_id
        try:
            user_pk = self._client.user_id_from_username(self.target_username)
            if not user_pk:
                raise InstagramBridgeError(
                    f"Could not resolve user ID for target @{self.target_username}."
                )
            self._target_user_id = str(user_pk)
            logger.info(
                f"Resolved target @{self.target_username} -> user_id={self._target_user_id}"
            )
            return self._target_user_id
        except Exception as err:
            raise InstagramBridgeError(
                f"Failed to lookup target Instagram user @{self.target_username}: {err}"
            ) from err

    def check_health(self) -> Tuple[bool, str]:
        """Returns current authentication status and target user resolution details."""
        if not self._is_authenticated:
            return False, "Not authenticated"
        if not self._target_user_id:
            return False, "Target user ID not resolved"
        return True, f"user_id={self._target_user_id}"

    def fetch_new_items(
        self,
        last_seen_item_id: Optional[str],
        known_thread_id: Optional[str] = None,
    ) -> Tuple[str, Optional[str], List[IgDirectItem]]:
        """
        Polls the Direct thread with target_username.
        Returns (thread_id, newest_item_id, chronological_new_items_from_target).
        """
        with self._lock:
            if not self._is_authenticated:
                raise InstagramBridgeError(
                    "Instagram client is not authenticated. Call authenticate() first."
                )

            target_uid = self._resolve_target_user_id_locked()

            try:
                if known_thread_id:
                    thread = self._client.direct_thread(
                        int(known_thread_id), amount=self.batch_fetch_limit
                    )
                else:
                    thread = self._client.direct_thread_by_participants(
                        [int(target_uid)]
                    )
            except Exception as err:
                raise InstagramBridgeError(
                    f"Failed to fetch Direct thread for @{self.target_username}: {err}"
                ) from err

            if not thread or not thread.id:
                raise InstagramBridgeError(
                    f"Direct thread with @{self.target_username} was not found."
                )

            thread_id = str(thread.id)
            messages = thread.messages or []

            if not messages:
                return thread_id, last_seen_item_id, []

            for msg in messages:
                if getattr(msg, "id", None):
                    self._cache_client_context(
                        str(msg.id), getattr(msg, "client_context", None)
                    )

            newest_item_id = str(messages[0].id)

            # On very first run without a persisted cursor, baseline at the newest message
            # to avoid flooding Telegram with historical messages.
            if last_seen_item_id is None:
                logger.info(
                    f"Initial synchronization baseline set for thread {thread_id} at item_id={newest_item_id}."
                )
                return thread_id, newest_item_id, []

            if newest_item_id == last_seen_item_id:
                return thread_id, newest_item_id, []

            new_raw_messages = []
            for msg in messages:
                msg_id = str(msg.id)
                if msg_id == last_seen_item_id:
                    break
                if str(getattr(msg, "user_id", "")) == target_uid:
                    new_raw_messages.append(msg)

            # Process in chronological order (oldest first)
            normalized_items: List[IgDirectItem] = []
            for raw_msg in reversed(new_raw_messages):
                normalized_items.append(
                    self._normalize_item_locked(raw_msg, thread_id, target_uid)
                )

            return thread_id, newest_item_id, normalized_items

    def _resolve_video_url_from_public_link_locked(
        self, public_url: Optional[str]
    ) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        """Fallback resolver that fetches (video_url, caption, author) via media_pk when XMA omits video_url."""
        if not public_url:
            return None, None, None
        match = REEL_CODE_REGEX.search(public_url)
        if not match:
            return None, None, None
        try:
            media_pk = self._client.media_pk_from_url(public_url)
            info = self._client.media_info(media_pk)
            video_url = str(info.video_url) if getattr(info, "video_url", None) else None
            caption = getattr(info, "caption_text", None)
            author = getattr(getattr(info, "user", None), "username", None)
            return video_url, caption, author
        except Exception as err:
            logger.warning(
                f"Could not resolve direct video stream via media_info for '{public_url}': {err}"
            )
            return None, None, None

    def _normalize_item_locked(
        self, msg: Any, thread_id: str, sender_uid: str
    ) -> IgDirectItem:
        """Converts raw instagrapi DirectMessage into a strongly-typed IgDirectItem DTO."""
        item_id = str(msg.id)
        raw_type = str(getattr(msg, "item_type", "unknown"))
        msg_text = getattr(msg, "text", None)
        if isinstance(msg_text, str):
            msg_text = msg_text.strip() or None

        replied_obj = getattr(msg, "replied_to_message", None) or getattr(
            msg, "reply_to_message", None
        )
        replied_to_id: Optional[str] = None
        if replied_obj is not None:
            if isinstance(replied_obj, dict) and replied_obj.get("id"):
                replied_to_id = str(replied_obj["id"])
            elif getattr(replied_obj, "id", None):
                replied_to_id = str(replied_obj.id)

        # 1. Plain text message
        if raw_type == "text":
            return IgDirectItem(
                item_id=item_id,
                thread_id=thread_id,
                sender_user_id=sender_uid,
                item_type=IgItemType.TEXT,
                raw_item_type=raw_type,
                text=msg_text or "",
                replied_to_item_id=replied_to_id,
            )

        # 2. Shared Reel / Clip (standard `clip` or 2026 `xma_clip` / `reel_share`)
        if raw_type in ("clip", "xma_clip", "reel_share"):
            clip_obj = getattr(msg, "clip", None)
            xma_obj = getattr(msg, "xma_share", None)

            code = getattr(clip_obj, "code", None) if clip_obj else None
            video_url = (
                str(clip_obj.video_url)
                if clip_obj and getattr(clip_obj, "video_url", None)
                else None
            )
            caption = getattr(clip_obj, "caption_text", None) if clip_obj else None
            author = (
                getattr(getattr(clip_obj, "user", None), "username", None)
                if clip_obj
                else None
            )
            public_url = f"https://www.instagram.com/reel/{code}/" if code else None

            if xma_obj is not None:
                if not public_url and getattr(xma_obj, "target_url", None):
                    public_url = str(xma_obj.target_url)
                if not video_url and getattr(xma_obj, "video_url", None):
                    video_url = str(xma_obj.video_url)
                if not caption and getattr(xma_obj, "title", None):
                    caption = str(xma_obj.title)

            if not public_url and isinstance(getattr(msg, "raw_xma", None), list):
                raw_xma_list = getattr(msg, "raw_xma")
                if raw_xma_list and isinstance(raw_xma_list[0], dict):
                    target_u = raw_xma_list[0].get("target_url") or raw_xma_list[0].get(
                        " playable_url"
                    )
                    if target_u:
                        public_url = str(target_u)

            if public_url and not video_url:
                resolved_v, resolved_c, resolved_a = (
                    self._resolve_video_url_from_public_link_locked(public_url)
                )
                video_url = video_url or resolved_v
                caption = caption or resolved_c
                author = author or resolved_a

            return IgDirectItem(
                item_id=item_id,
                thread_id=thread_id,
                sender_user_id=sender_uid,
                item_type=IgItemType.REEL,
                raw_item_type=raw_type,
                text=msg_text or caption,
                public_url=public_url or "https://www.instagram.com/",
                media_download_url=video_url,
                author_username=author,
                replied_to_item_id=replied_to_id,
            )

        # 3. Feed Post / Media Share
        if raw_type in ("media_share", "xma_media_share", "felix_share"):
            share_obj = (
                getattr(msg, "media_share", None)
                or getattr(msg, "felix_share", None)
                or getattr(msg, "xma_share", None)
            )
            code = getattr(share_obj, "code", None) if share_obj else None
            public_url = f"https://www.instagram.com/p/{code}/" if code else None
            if not public_url and share_obj and getattr(share_obj, "target_url", None):
                public_url = str(share_obj.target_url)

            media_url = None
            if share_obj:
                if getattr(share_obj, "video_url", None):
                    media_url = str(share_obj.video_url)
                elif getattr(share_obj, "thumbnail_url", None):
                    media_url = str(share_obj.thumbnail_url)

            author = (
                getattr(getattr(share_obj, "user", None), "username", None)
                if share_obj
                else None
            )
            caption = getattr(share_obj, "caption_text", None) if share_obj else None

            return IgDirectItem(
                item_id=item_id,
                thread_id=thread_id,
                sender_user_id=sender_uid,
                item_type=IgItemType.POST,
                raw_item_type=raw_type,
                text=msg_text or caption,
                public_url=public_url or "https://www.instagram.com/",
                media_download_url=media_url,
                author_username=author,
                replied_to_item_id=replied_to_id,
            )

        # 4. Direct Photo or Video sent in chat
        if raw_type in ("media", "raven_media", "visual_media"):
            media_obj = getattr(msg, "media", None)
            vis_obj = getattr(msg, "visual_media", None)
            if vis_obj and getattr(vis_obj, "media", None):
                media_obj = vis_obj.media

            if media_obj:
                m_type = getattr(media_obj, "media_type", 1)
                if m_type == 2 and getattr(media_obj, "video_url", None):
                    return IgDirectItem(
                        item_id=item_id,
                        thread_id=thread_id,
                        sender_user_id=sender_uid,
                        item_type=IgItemType.VIDEO,
                        raw_item_type=raw_type,
                        text=msg_text,
                        media_download_url=str(media_obj.video_url),
                        replied_to_item_id=replied_to_id,
                    )
                if getattr(media_obj, "thumbnail_url", None):
                    return IgDirectItem(
                        item_id=item_id,
                        thread_id=thread_id,
                        sender_user_id=sender_uid,
                        item_type=IgItemType.PHOTO,
                        raw_item_type=raw_type,
                        text=msg_text,
                        media_download_url=str(media_obj.thumbnail_url),
                        replied_to_item_id=replied_to_id,
                    )

        # 5. Voice Message
        if raw_type == "voice_media":
            voice_url: Optional[str] = None
            voice_obj = getattr(msg, "voice_media", None)
            if isinstance(voice_obj, dict):
                voice_url = (
                    voice_obj.get("media", {}).get("audio", {}).get("audio_src")
                )
            elif voice_obj is not None:
                media_sub = getattr(voice_obj, "media", None)
                audio_sub = getattr(media_sub, "audio", None) if media_sub else None
                voice_url = (
                    str(audio_sub.audio_src)
                    if audio_sub and getattr(audio_sub, "audio_src", None)
                    else None
                )

            if voice_url:
                return IgDirectItem(
                    item_id=item_id,
                    thread_id=thread_id,
                    sender_user_id=sender_uid,
                    item_type=IgItemType.VOICE,
                    raw_item_type=raw_type,
                    media_download_url=voice_url,
                    replied_to_item_id=replied_to_id,
                )

        # 6. Link share
        if raw_type == "link":
            link_obj = getattr(msg, "link", None)
            link_text = msg_text
            if not link_text and link_obj:
                link_ctx = getattr(link_obj, "link_context", None)
                link_url = getattr(link_ctx, "link_url", None) if link_ctx else None
                link_text = getattr(link_obj, "text", None) or link_url
            return IgDirectItem(
                item_id=item_id,
                thread_id=thread_id,
                sender_user_id=sender_uid,
                item_type=IgItemType.LINK,
                raw_item_type=raw_type,
                text=str(link_text or ""),
                replied_to_item_id=replied_to_id,
            )

        # 7. Fallback for unsupported attachment types
        return IgDirectItem(
            item_id=item_id,
            thread_id=thread_id,
            sender_user_id=sender_uid,
            item_type=IgItemType.UNSUPPORTED,
            raw_item_type=raw_type,
            text=msg_text,
            replied_to_item_id=replied_to_id,
        )

    def send_direct_message(
        self,
        thread_id: str,
        text: str,
        reply_to_item_id: Optional[str] = None,
    ) -> str:
        """
        Sends a text message or native swipe-reply into the target Direct thread.
        Returns the Instagram item_id of the sent message.
        """
        if not thread_id or not thread_id.strip():
            raise ValueError("Parameter 'thread_id' cannot be empty.")
        if not text or not text.strip():
            raise ValueError("Cannot send an empty text message to Instagram Direct.")

        with self._lock:
            if not self._is_authenticated:
                raise InstagramBridgeError(
                    "Instagram client is not authenticated. Cannot send Direct message."
                )

            reply_stub = None
            if reply_to_item_id:
                clean_reply_id = reply_to_item_id.strip()
                client_ctx = self._client_context_cache.get(clean_reply_id)
                if not client_ctx:
                    try:
                        orig_msg = self._client.direct_message(
                            int(thread_id), int(clean_reply_id)
                        )
                        if orig_msg and getattr(orig_msg, "client_context", None):
                            client_ctx = str(orig_msg.client_context)
                            self._cache_client_context(clean_reply_id, client_ctx)
                    except Exception as lookup_err:
                        logger.debug(
                            f"Could not fetch original client_context for item {clean_reply_id}: {lookup_err}"
                        )
                reply_stub = SimpleNamespace(
                    id=clean_reply_id,
                    client_context=client_ctx or clean_reply_id,
                )

            try:
                sent_msg = self._client.direct_send(
                    text=text.strip(),
                    thread_ids=[int(thread_id)],
                    reply_to_message=reply_stub,
                )
            except Exception as err:
                raise InstagramBridgeError(
                    f"Failed to send Direct message to thread {thread_id}: {err}"
                ) from err

            if not sent_msg or not getattr(sent_msg, "id", None):
                raise InstagramBridgeError(
                    "Instagram API returned an empty response without a message ID."
                )

            sent_id = str(sent_msg.id)
            self._cache_client_context(
                sent_id, getattr(sent_msg, "client_context", None)
            )
            return sent_id

    def download_media_to_temp(
        self, media_url: str, item_id: str, extension: str
    ) -> str:
        """
        Streams a remote Instagram media file into TMP_MEDIA_DIR with strict size and timeout bounds.
        Guarantees immediate file removal if download fails or exceeds TELEGRAM_MAX_VIDEO_BYTES.
        """
        if not media_url or not media_url.strip():
            raise ValueError("Parameter 'media_url' cannot be empty.")

        os.makedirs(TMP_MEDIA_DIR, exist_ok=True)
        clean_ext = extension if extension.startswith(".") else f".{extension}"
        safe_item_id = "".join(c for c in item_id if c.isalnum() or c in ("_", "-"))
        target_path = os.path.join(TMP_MEDIA_DIR, f"ig_{safe_item_id}{clean_ext}")

        total_bytes = 0
        try:
            with requests.get(
                media_url.strip(),
                stream=True,
                timeout=MEDIA_DOWNLOAD_TIMEOUT_SECONDS,
            ) as resp:
                resp.raise_for_status()
                with open(target_path, "wb") as out_file:
                    for chunk in resp.iter_content(
                        chunk_size=MEDIA_DOWNLOAD_CHUNK_SIZE
                    ):
                        if not chunk:
                            continue
                        total_bytes += len(chunk)
                        if total_bytes > TELEGRAM_MAX_VIDEO_BYTES:
                            raise InstagramBridgeError(
                                f"Media file exceeds Telegram 50 MB limit ({total_bytes} bytes)."
                            )
                        out_file.write(chunk)

            if total_bytes == 0:
                raise InstagramBridgeError("Downloaded media file is empty (0 bytes).")

            return target_path
        except Exception as err:
            if os.path.exists(target_path):
                try:
                    os.remove(target_path)
                except OSError as rm_err:
                    logger.warning(
                        f"Failed to remove partial media file '{target_path}': {rm_err}"
                    )
            if isinstance(err, InstagramBridgeError):
                raise
            raise InstagramBridgeError(
                f"Failed to download media for item {item_id}: {err}"
            ) from err


ig_bridge = InstagramBridgeClient()


if __name__ == "__main__":
    setup_logger("INFO")
    logger.info("Starting interactive Instagram session bootstrap...")
    ig_bridge.authenticate()
    thread_id, latest_item_id, _ = ig_bridge.fetch_new_items(last_seen_item_id=None)
    logger.info(
        f"Bootstrap complete! Target @{ig_bridge.target_username} Direct thread_id={thread_id}, "
        f"latest_item_id={latest_item_id}. Session persisted at '{IG_SESSION_PATH}'."
    )
