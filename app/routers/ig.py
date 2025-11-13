from fastapi import APIRouter
import logging

from .websocket_handlers import router as websocket_router
from .inbox_handlers import router as inbox_router
from .thread_handlers import router as thread_router
from .admin_handlers import router as admin_router
from .utils_handlers import router as utils_router

router = APIRouter(prefix="/ig", tags=["instagram"])
_log = logging.getLogger("instagram.inbox")

# Include all sub-routers
router.include_router(websocket_router)
router.include_router(inbox_router)
router.include_router(thread_router)
router.include_router(admin_router)
router.include_router(utils_router)
