"""HTTP API package: physically separate application and admin ASGI planes."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from queue_service.api.admin import create_admin_app as create_admin_app
    from queue_service.api.application import create_application_app as create_application_app
    from queue_service.api.security import ListenerBind as ListenerBind

__all__ = [
    "ListenerBind",
    "create_admin_app",
    "create_application_app",
]


def __getattr__(name: str) -> Any:
    if name == "ListenerBind":
        from queue_service.api.security import ListenerBind

        return ListenerBind
    if name == "create_admin_app":
        from queue_service.api.admin import create_admin_app

        return create_admin_app
    if name == "create_application_app":
        from queue_service.api.application import create_application_app

        return create_application_app
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
