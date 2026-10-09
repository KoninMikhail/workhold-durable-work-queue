"""Export guard: core must not ship role operation clients (SDK-04)."""

from __future__ import annotations

import importlib
import pkgutil

import pytest

FORBIDDEN_CLIENT_NAMES = (
    "ProducerClient",
    "ConsumerClient",
    "ObserverClient",
    "AdminClient",
    "BreakGlassClient",
    "WorkerClient",
    "WorkerSupervisor",
    "ConsumerSupervisor",
)


def test_core_package_importable() -> None:
    core = importlib.import_module("_queue_service_client_core")
    assert core.__name__ == "_queue_service_client_core"


def test_core_exports_no_role_operation_clients() -> None:
    core = importlib.import_module("_queue_service_client_core")
    public_names = set(dir(core)) | set(getattr(core, "__all__", ()))
    for name in FORBIDDEN_CLIENT_NAMES:
        assert name not in public_names, f"core leaked {name}"
        assert not hasattr(core, name), f"core attribute {name} must not exist"


def test_core_submodules_export_no_role_operation_clients() -> None:
    core = importlib.import_module("_queue_service_client_core")
    for mod_info in pkgutil.walk_packages(core.__path__, core.__name__ + "."):
        module = importlib.import_module(mod_info.name)
        for name in FORBIDDEN_CLIENT_NAMES:
            assert not hasattr(module, name), f"{mod_info.name} leaked {name}"


def test_core_has_shared_primitives() -> None:
    transport = importlib.import_module("_queue_service_client_core.transport")
    errors = importlib.import_module("_queue_service_client_core.errors")
    models = importlib.import_module("_queue_service_client_core.models")
    priority = importlib.import_module("_queue_service_client_core.priority")
    capabilities = importlib.import_module("_queue_service_client_core.capabilities")
    redaction = importlib.import_module("_queue_service_client_core.redaction")

    assert hasattr(transport, "HttpJsonTransport")
    assert hasattr(transport, "encode_path_segment")
    assert hasattr(errors, "ProtocolError")
    assert hasattr(errors, "AuthenticationError")
    assert hasattr(models, "Task")
    assert hasattr(models, "ProtocolErrorBody")
    assert hasattr(priority, "validate_priority")
    assert hasattr(capabilities, "Capabilities")
    assert hasattr(redaction, "redact_headers")
    assert hasattr(redaction, "sanitize_for_diagnostics")
