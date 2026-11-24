"""
routers — FastAPI route handlers.

  pipelines : CRUD, parse, execute, execution history
"""
from app.routers.pipelines import router as pipelines_router

__all__ = ["pipelines_router"]
