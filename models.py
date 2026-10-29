from dataclasses import dataclass
from enum import Enum
from typing import Optional


class IgItemType(str, Enum):
    """Normalized types of Instagram Direct messages handled by the bridge."""

    TEXT = "text"
    REEL = "reel"
    POST = "post"
    PHOTO = "photo"
    VIDEO = "video"
    VOICE = "voice"
    LINK = "link"
    RAVEN_EXPIRED = "raven_expired"
    UNSUPPORTED = "unsupported"


@dataclass(frozen=True)
class IgDirectItem:
    """Immutable DTO representing a single normalized Instagram Direct message."""

    item_id: str
    thread_id: str
    sender_user_id: str
    item_type: IgItemType
    raw_item_type: str
    text: Optional[str] = None
    public_url: Optional[str] = None
    media_download_url: Optional[str] = None
    author_username: Optional[str] = None
    replied_to_item_id: Optional[str] = None


@dataclass(frozen=True)
class PreparedTelegramVideo:
    """Immutable DTO holding normalized H.264 MP4 path, thumbnail path, and stream dimensions."""

    video_path: str
    thumb_path: Optional[str]
    width: int
    height: int
    duration: int
