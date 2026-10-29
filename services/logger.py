import logging
import sys
from typing import List, Optional, Union
from loguru import logger


class InterceptHandler(logging.Handler):
    """Intercepts standard logging records and routes them through Loguru."""

    def emit(self, record: logging.LogRecord) -> None:
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno

        frame, depth = logging.currentframe(), 2
        while frame and frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back
            depth += 1

        logger.opt(depth=depth, exception=record.exc_info).log(
            level, record.getMessage()
        )


def setup_logger(
    level: Union[str, int] = "INFO", ignored: Optional[List[str]] = None
) -> None:
    """Configures centralized structured logging for the bridge service."""
    logger.remove()
    logger.add(
        sys.stderr,
        level=level,
        format=(
            "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
            "<level>{level: <8}</level> | "
            "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
            "<level>{message}</level>"
        ),
    )

    logging.basicConfig(handlers=[InterceptHandler()], level=0, force=True)
    logging.getLogger("aiohttp.access").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    if ignored:
        for module_name in ignored:
            logger.disable(module_name)

    logger.info("Structured Loguru logging initialized successfully.")
