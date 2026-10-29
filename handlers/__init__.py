from aiogram import Router
from .commands import router as commands_router
from .bridge import router as bridge_router

main_router = Router(name="main_router")
main_router.include_router(commands_router)
main_router.include_router(bridge_router)

__all__ = ["main_router"]
