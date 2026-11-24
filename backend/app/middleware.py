"""
Centralized error-handling middleware.
Catches validation errors, HTTP exceptions, and unexpected crashes — always
returns a consistent JSON envelope: { "error": { "code", "message", "details" } }
"""
from __future__ import annotations

import logging
import traceback

from fastapi import Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import ValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException

logger = logging.getLogger("agentflow.middleware")


def _error_body(code: str, message: str, details=None) -> dict:
    return {"error": {"code": code, "message": message, "details": details}}


async def validation_exception_handler(request: Request, exc: RequestValidationError):
    logger.warning("Validation error on %s: %s", request.url, exc.errors())
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content=_error_body(
            code="VALIDATION_ERROR",
            message="Request body failed validation.",
            details=exc.errors(),
        ),
    )


async def http_exception_handler(request: Request, exc: StarletteHTTPException):
    logger.warning("HTTP %d on %s: %s", exc.status_code, request.url, exc.detail)
    return JSONResponse(
        status_code=exc.status_code,
        content=_error_body(
            code="HTTP_ERROR",
            message=str(exc.detail),
        ),
    )


async def unhandled_exception_handler(request: Request, exc: Exception):
    logger.error(
        "Unhandled exception on %s\n%s",
        request.url,
        traceback.format_exc(),
    )
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content=_error_body(
            code="INTERNAL_ERROR",
            message="An unexpected server error occurred.",
            details=str(exc),
        ),
    )
