# ==============================================================================
# ARCHITECTURAL INVARIANT: SINGLE SOURCE OF TRUTH (SSOT) FOR PRESENTATION LAYER
#
# RULE: 100% of user-facing strings, message templates, status cards, error
# alerts, and reaction emojis MUST reside strictly within this lexicon registry.
#
# FORBIDDEN:
# - Hardcoding natural language strings inside handlers, pollers, or services.
# - Constructing UI text via ad-hoc string concatenation outside this module.
# ==============================================================================

from typing import Dict
from aiogram import Bot
from aiogram.types import BotCommand

LEXICON: Dict[str, str] = {
    # Access Control
    "auth_access_denied": "⛔ <b>Доступ запрещен.</b> Этот мост является приватным.",
    # Commands: /start, /help, /status
    "cmd_start": (
        "🌉 <b>Instagram ↔ Telegram Bridge активен!</b>\n\n"
        "• Все сообщения, рилсы, посты, фото и голосовые от <b>@{target_username}</b> "
        "автоматически пересылаются сюда.\n"
        "• Чтобы ответить <b>цитатой (реплаем)</b> на конкретный рилс или сообщение в Instagram — "
        "просто сделайте <b>Reply (Ответить)</b> на него здесь в Telegram.\n"
        "• Если написать обычное сообщение без реплая — оно уйдет в ваш диалог Direct как новое сообщение."
    ),
    "cmd_help": (
        "📖 <b>Справка по работе моста:</b>\n\n"
        "1. <b>Входящие рилсы и видео:</b> Бот автоматически скачивает видеофайл рилса и присылает его "
        "нативным видео прямо в чат (плюс ссылка на оригинал и текст подписи).\n"
        "2. <b>Ответ реплаем:</b> Свайпните любое сообщение бота влево (Reply) и напишите текст — "
        "девушка увидит в Instagram Direct нативный ответ именно на этот рилс или сообщение.\n"
        "3. <b>Обычное сообщение:</b> Любой текст, отправленный без реплая, сразу отправляется в ваш Direct.\n"
        "4. <b>Статус доставки:</b> Когда бот успешно доставляет ваше сообщение в Instagram, "
        "он ставит реакцию 🫡 на ваше сообщение.\n\n"
        "Команды:\n"
        "• /status — состояние сессии Instagram и статистика БД\n"
        "• /help — эта справка"
    ),
    "cmd_status": (
        "📊 <b>Статус моста Instagram ↔ Telegram:</b>\n\n"
        "• Целевой профиль: <b>@{target_username}</b>\n"
        "• Сессия Instagram: <b>{ig_status}</b>\n"
        "• ID диалога Direct: <code>{thread_id}</code>\n"
        "• Последний курсор (Item ID): <code>{last_item_id}</code>\n"
        "• Последняя проверка: <b>{last_sync_str}</b>\n"
        "• Связок сообщений в БД: <b>{total_mappings}</b> шт."
    ),
    "status_ig_ok": "🟢 Активна",
    "status_ig_error": "🔴 Ошибка ({error})",
    "status_not_initialized": "Еще не синхронизировано",
    # Incoming Instagram Direct item templates
    "ig_in_text": "💬 <b>@{sender}:</b>\n{text}",
    "ig_in_reel_caption": (
        "🎬 <b>Рилс от @{sender}</b>{author_line}\n"
        "{text_line}"
        "🔗 <a href=\"{url}\">Открыть в Instagram</a>"
    ),
    "ig_in_reel_fallback_link": (
        "🎬 <b>Рилс от @{sender}</b> (не удалось скачать видеофайл){author_line}\n"
        "{text_line}"
        "🔗 {url}"
    ),
    "ig_in_post_caption": (
        "📸 <b>Пост от @{sender}</b>{author_line}\n"
        "{text_line}"
        "🔗 <a href=\"{url}\">Открыть пост в Instagram</a>"
    ),
    "ig_in_author_fmt": " (автор: <code>@{author}</code>)",
    "ig_in_text_fmt": "\n💬 <i>{text}</i>\n",
    "ig_in_photo_caption": "🖼 <b>Фото от @{sender}</b>{text_line}",
    "ig_in_video_caption": "📹 <b>Видео от @{sender}</b>{text_line}",
    "ig_in_voice_caption": "🎤 <b>Голосовое сообщение от @{sender}</b>",
    "ig_in_link": "🔗 <b>Ссылка от @{sender}:</b>\n{text}",
    "ig_in_raven_expired": (
        "💣 <b>Исчезающее фото/видео от @{sender}</b>\n"
        "<i>Медиафайл с одноразовым просмотром уже был открыт в приложении Instagram или истек.</i>"
    ),
    "ig_in_unsupported": (
        "📩 <b>Сообщение от @{sender}</b>\n"
        "<i>Тип вложения [{raw_type}] пока не отображается превью. Откройте Direct при необходимости.</i>"
    ),
    # Outgoing Telegram -> Instagram feedback
    "tg_out_reaction_ok": "🫡",
    "tg_out_reply_not_found": (
        "⚠️ <b>Не найден ID этого сообщения в базе.</b>\n"
        "Возможно, вы ответили на системное сообщение или запись старше 30 дней."
    ),
    "tg_out_no_thread_yet": (
        "⚠️ <b>Диалог Direct с @{target_username} еще не инициализирован.</b>\n"
        "Дождитесь первой синхронизации поллера (несколько секунд)."
    ),
    "tg_out_unsupported_content": (
        "⚠️ <b>Поддерживается отправка текста, фото и видео (стикеры и файлы отключены).</b>"
    ),
    "tg_out_send_error": (
        "❌ <b>Ошибка отправки сообщения в Instagram Direct:</b>\n"
        "<code>{error}</code>"
    ),
}

LEXICON_COMMANDS: Dict[str, str] = {
    "/status": "Статус соединения и синхронизации",
    "/help": "Как пользоваться мостом и реплаями",
}


def t(key: str, **kwargs) -> str:
    """Retrieves localized template by key with fail-fast validation."""
    if key not in LEXICON:
        raise KeyError(f"Lexicon key '{key}' is missing from LEXICON registry.")
    template = LEXICON[key]
    return template.format(**kwargs) if kwargs else template


async def set_bot_commands(bot: Bot) -> None:
    """Configures persistent command menu in Telegram client."""
    commands = [
        BotCommand(command=cmd.lstrip("/"), description=desc)
        for cmd, desc in LEXICON_COMMANDS.items()
    ]
    await bot.set_my_commands(commands)
