from fastapi import APIRouter
import logging

from .websocket_handlers import router as websocket_router
from .inbox_handlers import router as inbox_router
from .thread_handlers import router as thread_router
from .ig_inbox_extras import router as inbox_extras_router
from .ig_content import router as content_router
from .ig_insights_view import router as insights_router
from .ig_comments_view import router as comments_router
from .admin_handlers import router as admin_router
from .utils_handlers import router as utils_router
from .mock_tester import router as mock_tester_router

router = APIRouter(prefix="/ig", tags=["instagram"])
_log = logging.getLogger("instagram.inbox")

# Include all sub-routers
router.include_router(websocket_router)
router.include_router(inbox_router)
router.include_router(thread_router)
router.include_router(inbox_extras_router)
router.include_router(content_router)
router.include_router(insights_router)
router.include_router(comments_router)
router.include_router(admin_router)
router.include_router(utils_router)
router.include_router(mock_tester_router)
