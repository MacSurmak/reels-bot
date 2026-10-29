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
import sys
import json
import subprocess
import threading
from collections import OrderedDict
from types import SimpleNamespace
from typing import Optional, List, Tuple, Dict, Any
import requests
from PIL import Image
from instagrapi import Client
from instagrapi.exceptions import TwoFactorRequired
from instagrapi.extractors import extract_direct_thread
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
    FFMPEG_TIMEOUT_SECONDS,
    TELEGRAM_MAX_VIDEO_BYTES,
    SINGLE_PAGE_THREAD_AMOUNT,
)
from models import IgDirectItem, IgItemType, PreparedTelegramVideo
from services.logger import setup_logger
from storage import (
    STATE_KEY_TARGET_USER_ID_PREFIX,
    get_sync_state_sync,
    set_sync_state_sync,
)

CLIENT_CONTEXT_CACHE_SIZE = 500
REEL_CODE_REGEX = re.compile(r"instagram\.com/(?:reel|reels|p|tv)/([A-Za-z0-9_-]+)")
MEDIA_ID_QUERY_REGEX = re.compile(r"[?&]id=(\d+)")


class InstagramBridgeError(Exception):
    """Raised when an Instagram Direct operation, authentication, or media download fails."""


class InstagramBridgeClient:
    """Thread-safe Instagram Direct gateway managing session state, polling, and replies."""

    def __init__(self) -> None:
        ig_cfg = cfg["instagram"]
        self.username: str = ig_cfg["username"].strip()
        self.password: str = ig_cfg["password"].strip()
        self.target_username: str = ig_cfg["target_username"].strip().lstrip("@")
        raw_sid = ig_cfg.get("session_id")
        self.session_id: Optional[str] = (
            raw_sid.strip() if isinstance(raw_sid, str) and raw_sid.strip() else None
        )
        raw_target_uid = ig_cfg.get("target_user_id")
        self.configured_target_user_id: Optional[str] = (
            str(raw_target_uid).strip()
            if raw_target_uid is not None and str(raw_target_uid).strip()
            else None
        )
        self.batch_fetch_limit: int = int(cfg["polling"]["batch_fetch_limit"])

        self._lock = threading.Lock()
        self._client = self._create_configured_client()
        self._target_user_id: Optional[str] = None
        self._is_authenticated: bool = False
        self._client_context_cache: OrderedDict[str, str] = OrderedDict()

    @staticmethod
    def _normalize_session_id(raw_session_id: str) -> str:
        """Normalizes browser sessionid cookie (ensures %3A separator required by instagrapi)."""
        cleaned = raw_session_id.strip().strip('"').strip("'")
        if "%3A" not in cleaned and "%3a" not in cleaned and ":" in cleaned:
            cleaned = cleaned.replace(":", "%3A")
        return cleaned

    def _create_configured_client(self) -> Client:
        """Creates an instagrapi Client with native 2026 profile and interactive challenge handler."""
        cl = Client()
        cl.delay_range = [
            IG_REQUEST_DELAY_MIN_SECONDS,
            IG_REQUEST_DELAY_MAX_SECONDS,
        ]
        cl.challenge_code_handler = self._handle_challenge_code
        return cl

    def _handle_challenge_code(self, username: str, choice: Any = None) -> str:
        """
        Interactive callback invoked by instagrapi during CAA Bloks two-step verification
        (`com.bloks.www.ap.two_step_verification.entrypoint_async`) or challenge checkpoints.
        """
        if not sys.stdin.isatty():
            raise InstagramBridgeError(
                f"Instagram requested a verification code (choice={choice}) for @{username}, "
                "but no interactive TTY is attached. Run "
                "'docker compose run --rm ig-tg-bridge python3 -m services.ig_client' first."
            )
        logger.info(
            f"Instagram requested verification code for @{username} (method: {choice}). "
            "Check your email, SMS, WhatsApp, or Instagram app notifications."
        )
        code = input(
            f"👉 Enter verification code for @{username} (method={choice}): "
        ).strip()
        if not code:
            raise InstagramBridgeError(
                "Verification code cannot be empty. Authentication aborted."
            )
        return code

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
        """Authenticates with Instagram using persisted session settings, sessionid, or CAA login."""
        with self._lock:
            os.makedirs(DATA_DIR, exist_ok=True)

            if os.path.exists(IG_SESSION_PATH):
                logger.info(
                    f"Loading persisted Instagram session from '{IG_SESSION_PATH}'..."
                )
                try:
                    self._client = self._create_configured_client()
                    self._client.load_settings(
                        IG_SESSION_PATH, override_app_version=True
                    )
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

    def _login_with_session_id_locked(self, raw_sid: str) -> None:
        """Authenticates mobile client directly from a browser sessionid cookie."""
        clean_sid = self._normalize_session_id(raw_sid)
        logger.info(f"Authenticating @{self.username} via sessionid cookie...")
        self._client = self._create_configured_client()
        self._client.login_by_sessionid(clean_sid)
        self._client.dump_settings(IG_SESSION_PATH)
        self._is_authenticated = True
        logger.info(
            f"Sessionid authentication succeeded for @{self.username}. "
            f"Mobile session saved to '{IG_SESSION_PATH}'."
        )

    def _perform_fresh_login(self) -> None:
        """Executes fresh authentication via configured session_id, CAA Bloks, or interactive sessionid prompt."""
        try:
            if self.session_id:
                self._login_with_session_id_locked(self.session_id)
                return

            self._client = self._create_configured_client()
            self._client.username = self.username
            self._client.password = self.password

            try:
                outcome = self._client.bloks_caa_login()
            except TwoFactorRequired:
                if not sys.stdin.isatty():
                    raise InstagramBridgeError(
                        f"2FA is required for @{self.username}. Configure 'instagram.session_id' "
                        "in config.yaml or run interactive bootstrap."
                    )
                code = input(
                    f"👉 Enter 6-digit 2FA code for @{self.username}: "
                ).strip()
                outcome = self._client.bloks_caa_login(verification_code=code)

            if isinstance(outcome, dict) and outcome.get("logged_in"):
                self._client.dump_settings(IG_SESSION_PATH)
                self._is_authenticated = True
                logger.info(
                    f"CAA Bloks login succeeded for @{self.username}. "
                    f"Session saved to '{IG_SESSION_PATH}'."
                )
                return

            reason = (
                outcome.get("reason", "unknown")
                if isinstance(outcome, dict)
                else "unknown"
            )
            logger.warning(
                f"Cold CAA Bloks login did not return a session (reason='{reason}'). "
                "Bypassing dead legacy login endpoint."
            )

            if not sys.stdin.isatty():
                raise InstagramBridgeError(
                    f"Cold CAA login blocked by Instagram two-step verification (reason='{reason}'). "
                    "Set 'instagram.session_id' in config.yaml or run interactive bootstrap: "
                    "'docker compose run --rm ig-tg-bridge python3 -m services.ig_client'"
                )

            print(
                "\n======================================================================\n"
                "Instagram blocked cold password login with a Bloks verification screen.\n"
                "To initialize the mobile session immediately:\n"
                "1. Open https://www.instagram.com in your browser (logged into your account)\n"
                "2. Press F12 -> Application (or Storage) -> Cookies -> https://www.instagram.com\n"
                "3. Copy the value of the 'sessionid' cookie and paste it below\n"
                "   (OR enter your 6-digit TOTP authenticator code if using an authenticator app)\n"
                "======================================================================"
            )
            user_input = input(
                f"👉 Paste 'sessionid' cookie (or 6-digit 2FA code) for @{self.username}: "
            ).strip()

            if not user_input:
                raise InstagramBridgeError(
                    "Empty input provided. Authentication aborted."
                )

            if user_input.isdigit() and len(user_input) in (6, 8):
                retry_outcome = self._client.bloks_caa_login(
                    verification_code=user_input
                )
                if isinstance(retry_outcome, dict) and retry_outcome.get("logged_in"):
                    self._client.dump_settings(IG_SESSION_PATH)
                    self._is_authenticated = True
                    logger.info(
                        f"CAA 2FA login succeeded for @{self.username}. "
                        f"Session saved to '{IG_SESSION_PATH}'."
                    )
                    return
                raise InstagramBridgeError(
                    f"CAA login with 2FA code failed (reason='{retry_outcome.get('reason')}'). "
                    "Please use browser 'sessionid' instead."
                )

            self._login_with_session_id_locked(user_input)

        except InstagramBridgeError:
            self._is_authenticated = False
            raise
        except Exception as err:
            self._is_authenticated = False
            raise InstagramBridgeError(
                f"Failed to authenticate Instagram user @{self.username}: {err}"
            ) from err

    def _resolve_target_user_id_locked(self) -> str:
        """
        Resolves, caches, and persists numerical Instagram PK for target_username.
        Precedence:
        1. In-memory cached self._target_user_id.
        2. Configured 'instagram.target_user_id' in config.yaml.
        3. Persisted target user ID in SQLite sync_state table.
        4. Canonical mobile RPC user_info_by_username_v1 (persisted to SQLite atomically).
        """
        if self._target_user_id:
            return self._target_user_id

        db_key = f"{STATE_KEY_TARGET_USER_ID_PREFIX}{self.target_username}"

        if self.configured_target_user_id:
            self._target_user_id = self.configured_target_user_id
            set_sync_state_sync(db_key, self._target_user_id)
            logger.info(
                f"Target @{self.target_username} user_id={self._target_user_id} loaded from config (network lookup bypassed)."
            )
            return self._target_user_id

        cached_uid = get_sync_state_sync(db_key)
        if cached_uid:
            self._target_user_id = cached_uid
            logger.info(
                f"Loaded target @{self.target_username} -> user_id={self._target_user_id} from SQLite sync_state (network lookup bypassed)."
            )
            return self._target_user_id

        logger.info(
            f"Resolving numerical user_id for target @{self.target_username} via mobile endpoint..."
        )
        try:
            user_info = self._client.user_info_by_username_v1(self.target_username)
        except Exception as err:
            raise InstagramBridgeError(
                f"Failed to lookup target Instagram user @{self.target_username}: {err}"
            ) from err

        user_pk = getattr(user_info, "pk", None)
        if user_pk is None and isinstance(user_info, dict):
            user_pk = user_info.get("pk") or user_info.get("id")

        if not user_pk or not str(user_pk).strip():
            raise InstagramBridgeError(
                f"Could not resolve user ID for target @{self.target_username}: "
                f"user_info_by_username_v1 returned empty or invalid pk ({user_pk})."
            )

        self._target_user_id = str(user_pk).strip()
        set_sync_state_sync(db_key, self._target_user_id)
        logger.info(
            f"Resolved target @{self.target_username} -> user_id={self._target_user_id} via mobile RPC and persisted to SQLite."
        )
        return self._target_user_id

    def check_health(self) -> Tuple[bool, str]:
        """Returns current authentication status and target user resolution details."""
        if not self._is_authenticated:
            return False, "Not authenticated"
        if not self._target_user_id:
            return False, "Target user ID not resolved"
        return True, f"user_id={self._target_user_id}"

    def _resolve_thread_id_locked(self, target_uid: str) -> str:
        """Resolves Direct thread_id for target_uid from participant lookup or inbox scan."""
        res = self._client.direct_thread_by_participants([int(target_uid)])
        resolved_id: Optional[str] = None

        if isinstance(res, dict):
            thread_sub = res.get("thread")
            if isinstance(thread_sub, dict):
                raw_id = (
                    thread_sub.get("thread_id")
                    or thread_sub.get("id")
                    or thread_sub.get("thread_v2_id")
                )
                if raw_id:
                    resolved_id = str(raw_id)
            else:
                raw_id = res.get("thread_id") or res.get("id")
                if raw_id:
                    resolved_id = str(raw_id)
        elif getattr(res, "id", None):
            resolved_id = str(res.id)

        if not resolved_id:
            threads = self._client.direct_threads(amount=30)
            for t_obj in threads:
                users = getattr(t_obj, "users", []) or []
                if any(str(getattr(u, "pk", "")) == target_uid for u in users):
                    if getattr(t_obj, "id", None):
                        resolved_id = str(t_obj.id)
                        break

        if not resolved_id:
            raise InstagramBridgeError(
                f"Direct thread with @{self.target_username} (user_id={target_uid}) was not found."
            )
        return resolved_id

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

            raw_items_by_id: Dict[str, Dict[str, Any]] = {}
            try:
                active_thread_id = (
                    str(known_thread_id).strip()
                    if known_thread_id
                    else self._resolve_thread_id_locked(target_uid)
                )
                fetch_amount = (
                    min(self.batch_fetch_limit, SINGLE_PAGE_THREAD_AMOUNT)
                    if last_seen_item_id
                    else SINGLE_PAGE_THREAD_AMOUNT
                )
                raw_resp = self._client.private_request(
                    f"direct_v2/threads/{active_thread_id}/",
                    params={
                        "direction": "older",
                        "limit": str(fetch_amount),
                    },
                )
                raw_thread = (
                    raw_resp.get("thread") if isinstance(raw_resp, dict) else None
                )
                if isinstance(raw_thread, dict):
                    for raw_it in raw_thread.get("items") or []:
                        if isinstance(raw_it, dict) and raw_it.get("item_id"):
                            raw_items_by_id[str(raw_it["item_id"])] = raw_it
                    thread = extract_direct_thread(raw_thread)
                else:
                    thread = self._client.direct_thread(
                        int(active_thread_id), amount=fetch_amount
                    )
            except InstagramBridgeError:
                raise
            except Exception as err:
                raise InstagramBridgeError(
                    f"Failed to fetch Direct thread for @{self.target_username}: {err}"
                ) from err

            if not thread or not getattr(thread, "id", None):
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
                raw_dict = raw_items_by_id.get(str(raw_msg.id))
                normalized_items.append(
                    self._normalize_item_locked(
                        raw_msg, thread_id, target_uid, raw_dict
                    )
                )

            return thread_id, newest_item_id, normalized_items

    @staticmethod
    def _is_instagram_webpage_url(url: Optional[str]) -> bool:
        """Returns True if the URL points to an Instagram web page rather than a CDN media stream."""
        if not url:
            return False
        lowered = url.lower()
        return any(
            marker in lowered
            for marker in (
                "instagram.com/reel/",
                "instagram.com/reels/",
                "instagram.com/p/",
                "instagram.com/tv/",
                "instagram.com/share/",
            )
        )

    @staticmethod
    def _canonicalize_public_url(raw_url: str, default_kind: str = "reel") -> str:
        """Extracts clean canonical Instagram URL without tracking query parameters."""
        match = REEL_CODE_REGEX.search(raw_url)
        if match:
            code = match.group(1)
            prefix = "p" if "/p/" in raw_url.lower() else default_kind
            return f"https://www.instagram.com/{prefix}/{code}/"
        return raw_url

    def _resolve_media_from_public_link_locked(
        self, public_url: Optional[str]
    ) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
        """
        Resolves direct CDN (video_url, photo_url, caption, author) via authenticated mobile media_info_v1.
        Supports single Reels, single photo posts, and multi-slide carousel posts.
        """
        if not public_url:
            return None, None, None, None

        media_pk: Optional[int] = None
        id_match = MEDIA_ID_QUERY_REGEX.search(public_url)
        if id_match:
            media_pk = int(id_match.group(1))
        else:
            code_match = REEL_CODE_REGEX.search(public_url)
            if code_match:
                try:
                    media_pk = int(self._client.media_pk_from_code(code_match.group(1)))
                except Exception:
                    media_pk = int(self._client.media_pk_from_url(public_url))

        if not media_pk:
            return None, None, None, None

        try:
            info = self._client.media_info_v1(media_pk)
            video_url = (
                str(info.video_url) if getattr(info, "video_url", None) else None
            )
            photo_url = (
                str(info.thumbnail_url)
                if getattr(info, "thumbnail_url", None)
                else None
            )

            if getattr(info, "resources", None):
                for res in info.resources:
                    if not video_url and getattr(res, "video_url", None):
                        video_url = str(res.video_url)
                    if not photo_url and getattr(res, "thumbnail_url", None):
                        photo_url = str(res.thumbnail_url)
                    if video_url or photo_url:
                        break

            caption = getattr(info, "caption_text", None)
            author = getattr(getattr(info, "user", None), "username", None)
            logger.info(
                f"Resolved media_pk={media_pk} via media_info_v1 "
                f"(author=@{author}, has_video={bool(video_url)}, has_photo={bool(photo_url)})"
            )
            return video_url, photo_url, caption, author
        except Exception as err:
            logger.warning(
                f"Could not resolve media via media_info_v1 for '{public_url}' "
                f"(media_pk={media_pk}): {err}"
            )
            return None, None, None, None

    @classmethod
    def _extract_urls_from_raw_media_dict(
        cls, data: Any
    ) -> Tuple[Optional[str], Optional[str]]:
        """Recursively searches a raw Instagram JSON structure for video_versions and image_versions2."""
        if not isinstance(data, dict):
            return None, None

        video_url: Optional[str] = None
        photo_url: Optional[str] = None

        vid_versions = data.get("video_versions")
        if isinstance(vid_versions, list) and vid_versions:
            valid_vids = [
                v for v in vid_versions if isinstance(v, dict) and v.get("url")
            ]
            if valid_vids:
                best_vid = max(
                    valid_vids,
                    key=lambda x: int(x.get("width") or 0) * int(x.get("height") or 0),
                )
                video_url = str(best_vid["url"])

        img_versions2 = data.get("image_versions2")
        if isinstance(img_versions2, dict):
            candidates = img_versions2.get("candidates")
            if isinstance(candidates, list) and candidates:
                valid_imgs = [
                    c for c in candidates if isinstance(c, dict) and c.get("url")
                ]
                if valid_imgs:
                    best_img = max(
                        valid_imgs,
                        key=lambda x: int(x.get("width") or 0)
                        * int(x.get("height") or 0),
                    )
                    photo_url = str(best_img["url"])

        if video_url or photo_url:
            return video_url, photo_url

        for sub_key in ("visual_media", "raven_media", "media"):
            sub_val = data.get(sub_key)
            if isinstance(sub_val, dict):
                v_url, p_url = cls._extract_urls_from_raw_media_dict(sub_val)
                if v_url or p_url:
                    return v_url, p_url

        return None, None

    def _resolve_raven_media_locked(
        self,
        thread_id: str,
        item_id: str,
        raw_item_dict: Optional[Dict[str, Any]],
    ) -> Tuple[Optional[str], Optional[str]]:
        """Resolves (video_url, photo_url) for raven_media / visual_media via raw dict or get_items API."""
        if raw_item_dict:
            v_url, p_url = self._extract_urls_from_raw_media_dict(raw_item_dict)
            if v_url or p_url:
                return v_url, p_url

        try:
            resp = self._client.private_request(
                f"direct_v2/threads/{thread_id}/get_items/",
                params={
                    "item_ids": f"[{item_id}]",
                    "original_message_client_context": '[""]',
                },
            )
            if isinstance(resp, dict):
                items = resp.get("items") or []
                for it in items:
                    v_url, p_url = self._extract_urls_from_raw_media_dict(it)
                    if v_url or p_url:
                        return v_url, p_url
        except Exception as err:
            logger.warning(
                f"Fallback get_items lookup failed for raven_media #{item_id}: {err}"
            )

        return None, None

    def _normalize_item_locked(
        self,
        msg: Any,
        thread_id: str,
        sender_uid: str,
        raw_item_dict: Optional[Dict[str, Any]] = None,
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
            raw_clip_video = (
                str(clip_obj.video_url)
                if clip_obj and getattr(clip_obj, "video_url", None)
                else None
            )
            video_url = (
                None
                if self._is_instagram_webpage_url(raw_clip_video)
                else raw_clip_video
            )
            caption = getattr(clip_obj, "caption_text", None) if clip_obj else None
            author = (
                getattr(getattr(clip_obj, "user", None), "username", None)
                if clip_obj
                else None
            )
            raw_public_url = f"https://www.instagram.com/reel/{code}/" if code else None
            if not raw_public_url and self._is_instagram_webpage_url(raw_clip_video):
                raw_public_url = raw_clip_video

            if xma_obj is not None:
                # Note: instagrapi's extract_direct_xma maps Instagram's `target_url`
                # (the Reel webpage link) into `xma_share.video_url`.
                xma_target = (
                    str(xma_obj.target_url)
                    if getattr(xma_obj, "target_url", None)
                    else None
                )
                xma_video = (
                    str(xma_obj.video_url)
                    if getattr(xma_obj, "video_url", None)
                    else None
                )

                for candidate in (xma_target, xma_video):
                    if not candidate:
                        continue
                    if self._is_instagram_webpage_url(candidate):
                        if not raw_public_url:
                            raw_public_url = candidate
                    elif not video_url:
                        video_url = candidate

                if not caption and getattr(xma_obj, "title", None):
                    caption = str(xma_obj.title)
                if not author and getattr(xma_obj, "header_title_text", None):
                    author = str(xma_obj.header_title_text)

            if raw_public_url and not video_url:
                resolved_v, _, resolved_c, resolved_a = (
                    self._resolve_media_from_public_link_locked(raw_public_url)
                )
                video_url = video_url or resolved_v
                caption = caption or resolved_c
                author = author or resolved_a

            clean_public_url = (
                self._canonicalize_public_url(raw_public_url, default_kind="reel")
                if raw_public_url
                else "https://www.instagram.com/"
            )

            return IgDirectItem(
                item_id=item_id,
                thread_id=thread_id,
                sender_user_id=sender_uid,
                item_type=IgItemType.REEL,
                raw_item_type=raw_type,
                text=msg_text or caption,
                public_url=clean_public_url,
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
            raw_public_url = f"https://www.instagram.com/p/{code}/" if code else None

            media_url = None
            if share_obj:
                for attr in ("target_url", "video_url", "thumbnail_url", "preview_url"):
                    val = getattr(share_obj, attr, None)
                    if not val:
                        continue
                    s_val = str(val)
                    if self._is_instagram_webpage_url(s_val):
                        if not raw_public_url:
                            raw_public_url = s_val
                    elif not media_url:
                        media_url = s_val

            author = (
                getattr(getattr(share_obj, "user", None), "username", None)
                if share_obj
                else None
            )
            caption = getattr(share_obj, "caption_text", None) if share_obj else None

            if raw_public_url:
                resolved_v, resolved_p, resolved_c, resolved_a = (
                    self._resolve_media_from_public_link_locked(raw_public_url)
                )
                media_url = resolved_v or resolved_p or media_url
                caption = caption or resolved_c
                author = author or resolved_a

            clean_public_url = (
                self._canonicalize_public_url(raw_public_url, default_kind="p")
                if raw_public_url
                else "https://www.instagram.com/"
            )

            return IgDirectItem(
                item_id=item_id,
                thread_id=thread_id,
                sender_user_id=sender_uid,
                item_type=IgItemType.POST,
                raw_item_type=raw_type,
                text=msg_text or caption,
                public_url=clean_public_url,
                media_download_url=media_url,
                author_username=author,
                replied_to_item_id=replied_to_id,
            )

        # 4. Direct Photo or Video sent in chat (including Keep-in-chat and Raven camera media)
        if raw_type in ("media", "raven_media", "visual_media"):
            media_obj = getattr(msg, "media", None)
            vis_obj = getattr(msg, "visual_media", None)
            if vis_obj and getattr(vis_obj, "media", None):
                media_obj = vis_obj.media

            video_url: Optional[str] = None
            photo_url: Optional[str] = None

            if media_obj:
                if getattr(media_obj, "video_url", None):
                    video_url = str(media_obj.video_url)
                if getattr(media_obj, "thumbnail_url", None):
                    photo_url = str(media_obj.thumbnail_url)

            if not video_url and not photo_url:
                video_url, photo_url = self._resolve_raven_media_locked(
                    thread_id, item_id, raw_item_dict
                )

            if video_url:
                return IgDirectItem(
                    item_id=item_id,
                    thread_id=thread_id,
                    sender_user_id=sender_uid,
                    item_type=IgItemType.VIDEO,
                    raw_item_type=raw_type,
                    text=msg_text,
                    media_download_url=video_url,
                    replied_to_item_id=replied_to_id,
                )

            if photo_url:
                return IgDirectItem(
                    item_id=item_id,
                    thread_id=thread_id,
                    sender_user_id=sender_uid,
                    item_type=IgItemType.PHOTO,
                    raw_item_type=raw_type,
                    text=msg_text,
                    media_download_url=photo_url,
                    replied_to_item_id=replied_to_id,
                )

            if raw_type in ("raven_media", "visual_media"):
                return IgDirectItem(
                    item_id=item_id,
                    thread_id=thread_id,
                    sender_user_id=sender_uid,
                    item_type=IgItemType.RAVEN_EXPIRED,
                    raw_item_type=raw_type,
                    text=msg_text,
                    replied_to_item_id=replied_to_id,
                )

        # 5. Voice Message
        if raw_type == "voice_media":
            voice_url: Optional[str] = None
            voice_obj = getattr(msg, "voice_media", None)
            if isinstance(voice_obj, dict):
                voice_url = voice_obj.get("media", {}).get("audio", {}).get("audio_src")
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

    def send_direct_photo(self, thread_id: str, file_path: str) -> str:
        """
        Normalizes an image to RGB JPEG and sends it into the target Instagram Direct thread.
        Returns the Instagram item_id of the sent photo message.
        """
        if not thread_id or not thread_id.strip():
            raise ValueError("Parameter 'thread_id' cannot be empty.")
        if not file_path or not os.path.exists(file_path):
            raise ValueError(f"Photo file does not exist: '{file_path}'")

        os.makedirs(TMP_MEDIA_DIR, exist_ok=True)
        norm_jpg_path = f"{file_path}_norm.jpg"
        try:
            with Image.open(file_path) as img:
                rgb_img = img.convert("RGB")
                rgb_img.save(norm_jpg_path, format="JPEG", quality=92)

            with self._lock:
                if not self._is_authenticated:
                    raise InstagramBridgeError(
                        "Instagram client is not authenticated. Cannot send photo."
                    )
                sent_msg = self._client.direct_send_photo(
                    path=norm_jpg_path,
                    thread_ids=[int(thread_id)],
                )

            if not sent_msg or not getattr(sent_msg, "id", None):
                raise InstagramBridgeError(
                    "Instagram API returned an empty response when sending photo."
                )

            sent_id = str(sent_msg.id)
            self._cache_client_context(
                sent_id, getattr(sent_msg, "client_context", None)
            )
            return sent_id
        except InstagramBridgeError:
            raise
        except Exception as err:
            raise InstagramBridgeError(
                f"Failed to send photo to Direct thread {thread_id}: {err}"
            ) from err
        finally:
            if os.path.exists(norm_jpg_path):
                try:
                    os.remove(norm_jpg_path)
                except OSError:
                    pass

    def send_direct_video(self, thread_id: str, file_path: str) -> str:
        """
        Normalizes an outgoing video via ffmpeg to H.264/AAC MP4 and sends it into Instagram Direct.
        Returns the Instagram item_id of the sent video message.
        """
        if not thread_id or not thread_id.strip():
            raise ValueError("Parameter 'thread_id' cannot be empty.")
        if not file_path or not os.path.exists(file_path):
            raise ValueError(f"Video file does not exist: '{file_path}'")

        os.makedirs(TMP_MEDIA_DIR, exist_ok=True)
        norm_mp4_path = f"{file_path}_out_h264.mp4"
        try:
            ffmpeg_cmd = [
                "ffmpeg",
                "-y",
                "-v",
                "error",
                "-i",
                file_path,
                "-vf",
                "scale=trunc(iw/2)*2:trunc(ih/2)*2,format=yuv420p",
                "-c:v",
                "libx264",
                "-preset",
                "veryfast",
                "-crf",
                "23",
                "-c:a",
                "aac",
                "-b:a",
                "128k",
                "-movflags",
                "+faststart",
                norm_mp4_path,
            ]
            subprocess.run(
                ffmpeg_cmd,
                capture_output=True,
                text=True,
                timeout=FFMPEG_TIMEOUT_SECONDS,
                check=True,
            )

            with self._lock:
                if not self._is_authenticated:
                    raise InstagramBridgeError(
                        "Instagram client is not authenticated. Cannot send video."
                    )
                sent_msg = self._client.direct_send_video(
                    path=norm_mp4_path,
                    thread_ids=[int(thread_id)],
                )

            if not sent_msg or not getattr(sent_msg, "id", None):
                raise InstagramBridgeError(
                    "Instagram API returned an empty response when sending video."
                )

            sent_id = str(sent_msg.id)
            self._cache_client_context(
                sent_id, getattr(sent_msg, "client_context", None)
            )
            return sent_id
        except InstagramBridgeError:
            raise
        except Exception as err:
            raise InstagramBridgeError(
                f"Failed to send video to Direct thread {thread_id}: {err}"
            ) from err
        finally:
            if os.path.exists(norm_mp4_path):
                try:
                    os.remove(norm_mp4_path)
                except OSError:
                    pass

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

        headers = {
            "User-Agent": getattr(
                self._client,
                "user_agent",
                "Mozilla/5.0 (Linux; Android 14; Pixel 8 Pro) AppleWebKit/537.36",
            )
        }
        total_bytes = 0
        try:
            with requests.get(
                media_url.strip(),
                headers=headers,
                stream=True,
                timeout=MEDIA_DOWNLOAD_TIMEOUT_SECONDS,
            ) as resp:
                resp.raise_for_status()
                content_type = (resp.headers.get("Content-Type") or "").lower()
                if "text/html" in content_type:
                    raise InstagramBridgeError(
                        f"URL returned an HTML page instead of a media stream (Content-Type='{content_type}')."
                    )
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

    @staticmethod
    def _probe_video_stream(file_path: str) -> Optional[Dict[str, Any]]:
        """
        Inspects media file via ffprobe and returns video stream metadata
        (codec_name, pix_fmt, width, height, duration) or None if not a valid video.
        """
        cmd = [
            "ffprobe",
            "-v",
            "error",
            "-show_entries",
            "format=duration:stream=index,codec_type,codec_name,pix_fmt,width,height",
            "-of",
            "json",
            file_path,
        ]
        try:
            proc = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=MEDIA_DOWNLOAD_TIMEOUT_SECONDS,
                check=True,
            )
            data = json.loads(proc.stdout)
        except Exception as err:
            logger.warning(f"ffprobe failed on '{file_path}': {err}")
            return None

        streams = data.get("streams") or []
        video_stream = next(
            (s for s in streams if s.get("codec_type") == "video"), None
        )
        if not video_stream:
            return None

        codec_name = str(video_stream.get("codec_name") or "").lower()
        if codec_name in ("mjpeg", "png", "webp", "gif", "bmp"):
            return None

        width = int(video_stream.get("width") or 0)
        height = int(video_stream.get("height") or 0)
        pix_fmt = str(video_stream.get("pix_fmt") or "").lower()

        fmt_duration = data.get("format", {}).get("duration")
        duration_sec = float(fmt_duration) if fmt_duration is not None else 0.0
        if width <= 0 or height <= 0:
            return None

        return {
            "codec_name": codec_name,
            "pix_fmt": pix_fmt,
            "width": width,
            "height": height,
            "duration": max(1, int(round(duration_sec))),
        }

    def prepare_video_for_telegram(self, item: IgDirectItem) -> PreparedTelegramVideo:
        """
        Downloads an Instagram Reel/Video, verifies its stream via ffprobe,
        transcodes VP9/AV1/HEVC streams into Telegram-compliant H.264 (yuv420p) + AAC + faststart,
        and extracts a JPEG thumbnail.
        """
        if not item.media_download_url:
            raise InstagramBridgeError(
                f"Item {item.item_id} has no media_download_url."
            )

        raw_path: Optional[str] = None
        norm_path: Optional[str] = None
        thumb_path: Optional[str] = None

        try:
            raw_path = self.download_media_to_temp(
                item.media_download_url, f"{item.item_id}_raw", ".mp4"
            )
            probe = self._probe_video_stream(raw_path)

            # If primary URL returned a static cover image instead of video, resolve via media_info
            if probe is None and item.public_url:
                logger.info(
                    f"Primary download for item {item.item_id} is not a video stream. "
                    f"Resolving video URL via media_info for '{item.public_url}'..."
                )
                with self._lock:
                    resolved_url, _, _, _ = self._resolve_media_from_public_link_locked(
                        item.public_url
                    )
                if resolved_url and resolved_url != item.media_download_url:
                    if os.path.exists(raw_path):
                        os.remove(raw_path)
                    raw_path = self.download_media_to_temp(
                        resolved_url, f"{item.item_id}_raw", ".mp4"
                    )
                    probe = self._probe_video_stream(raw_path)

            if probe is None:
                raise InstagramBridgeError(
                    f"Downloaded file for item {item.item_id} does not contain a playable video stream."
                )

            logger.info(
                f"Probed Instagram video #{item.item_id}: codec={probe['codec_name']}, "
                f"pix_fmt={probe['pix_fmt']}, {probe['width']}x{probe['height']}, "
                f"duration={probe['duration']}s"
            )

            safe_id = "".join(c for c in item.item_id if c.isalnum() or c in ("_", "-"))
            norm_path = os.path.join(TMP_MEDIA_DIR, f"ig_{safe_id}_h264.mp4")
            thumb_path = os.path.join(TMP_MEDIA_DIR, f"ig_{safe_id}_thumb.jpg")

            # Transcode to H.264 (yuv420p) + AAC + faststart for universal Telegram playback
            ffmpeg_cmd = [
                "ffmpeg",
                "-y",
                "-v",
                "error",
                "-i",
                raw_path,
                "-vf",
                "scale=trunc(iw/2)*2:trunc(ih/2)*2,format=yuv420p",
                "-c:v",
                "libx264",
                "-preset",
                "veryfast",
                "-crf",
                "23",
                "-profile:v",
                "high",
                "-level",
                "4.1",
                "-c:a",
                "aac",
                "-b:a",
                "128k",
                "-movflags",
                "+faststart",
                norm_path,
            ]
            subprocess.run(
                ffmpeg_cmd,
                capture_output=True,
                text=True,
                timeout=FFMPEG_TIMEOUT_SECONDS,
                check=True,
            )

            final_probe = self._probe_video_stream(norm_path) or probe

            # Extract first-frame JPEG thumbnail for Telegram video player preview
            thumb_cmd = [
                "ffmpeg",
                "-y",
                "-v",
                "error",
                "-i",
                norm_path,
                "-ss",
                "00:00:00.200",
                "-frames:v",
                "1",
                "-vf",
                "scale='min(320,iw)':-2",
                thumb_path,
            ]
            subprocess.run(
                thumb_cmd,
                capture_output=True,
                text=True,
                timeout=MEDIA_DOWNLOAD_TIMEOUT_SECONDS,
                check=False,
            )
            if not os.path.exists(thumb_path) or os.path.getsize(thumb_path) == 0:
                thumb_path = None

            return PreparedTelegramVideo(
                video_path=norm_path,
                thumb_path=thumb_path,
                width=final_probe["width"],
                height=final_probe["height"],
                duration=final_probe["duration"],
            )
        except Exception as err:
            for p in (norm_path, thumb_path):
                if p and os.path.exists(p):
                    try:
                        os.remove(p)
                    except OSError:
                        pass
            if isinstance(err, InstagramBridgeError):
                raise
            raise InstagramBridgeError(
                f"Failed to normalize video for item {item.item_id}: {err}"
            ) from err
        finally:
            if raw_path and os.path.exists(raw_path):
                try:
                    os.remove(raw_path)
                except OSError as rm_err:
                    logger.warning(
                        f"Failed to remove raw video file '{raw_path}': {rm_err}"
                    )


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
