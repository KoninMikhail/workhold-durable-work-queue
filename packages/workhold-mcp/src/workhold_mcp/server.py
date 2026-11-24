"""Register the selected MCP tools and dispatch them to role clients."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Annotated, Any

from _workhold_client_core.transport import HttpJsonTransport
from mcp.server import MCPServer
from mcp.server.auth.provider import TokenVerifier
from mcp.server.auth.settings import AuthSettings
from mcp.types import CallToolResult, TextContent, ToolAnnotations
from pydantic import Field
from workhold_admin.admin import AdminClient
from workhold_admin.observer import ObserverClient
from workhold_producer.client import ProducerClient

from workhold_mcp.config import (
    ConfigError,
    McpSettings,
    origin_config,
    selection_needs,
)
from workhold_mcp.errors import project_error
from workhold_mcp.registry import REGISTRY, REGISTRY_BY_NAME, ToolSpec
from workhold_mcp.serialize import canonical_json, to_tool_payload

_PUBLIC_BASE_URL = "WORKHOLD_MCP_PUBLIC_BASE_URL"
_ADMIN_BASE_URL = "WORKHOLD_MCP_ADMIN_BASE_URL"
_OBSERVER_TOKEN = "WORKHOLD_MCP_OBSERVER_TOKEN"
_PRODUCER_TOKEN = "WORKHOLD_MCP_PRODUCER_TOKEN"
_ADMIN_TOKEN = "WORKHOLD_MCP_ADMIN_TOKEN"
_CONTENT_KEY = "WORKHOLD_MCP_CONTENT_KEY"


def build_server(
    settings: McpSettings,
    names: frozenset[str],
    *,
    token_verifier: TokenVerifier | None = None,
    auth: AuthSettings | None = None,
) -> MCPServer:
    """Register the selected registry tools and refuse a semantic schema drift.

    Stdio passes neither ``token_verifier`` nor ``auth``. Streamable HTTP passes
    both onto this same server. Passing only one of them raises ``ValueError``.
    """

    if (token_verifier is None) != (auth is None):
        raise ValueError("token_verifier and auth must be supplied together")
    server = MCPServer(
        "workhold-mcp",
        subscriptions=False,
        token_verifier=token_verifier,
        auth=auth,
    )
    clients = _role_clients(settings, names)
    selected = [spec for spec in REGISTRY if spec.name in names]
    selected.sort(key=lambda spec: spec.name)
    for spec in selected:
        client = clients.get(spec.surface)
        if client is None:
            raise ConfigError(_setting_for_surface(spec.surface))
        server.add_tool(
            _tool_function(spec, client, names),
            name=spec.name,
            description=spec.description,
            annotations=ToolAnnotations(
                read_only_hint=spec.read_only_hint,
                open_world_hint=spec.open_world_hint,
            ),
        )
    _check_published_schemas(server)
    return server


def call_tool(
    client: Any,
    name: str,
    arguments: Mapping[str, Any] | None = None,
    *,
    selected: frozenset[str] | None = None,
) -> CallToolResult:
    """Dispatch one registry call and project success or a bounded failure."""

    if selected is not None and name not in selected:
        return _error_result(ValueError("unknown tool"))
    spec = REGISTRY_BY_NAME.get(name)
    if spec is None:
        return _error_result(ValueError("unknown tool"))
    try:
        value = spec.call(client, **dict(arguments or {}))
    except Exception as exc:
        return _error_result(exc)
    return _success_result(to_tool_payload(value))


def assert_published_schema(tool_name: str, published: Mapping[str, Any]) -> None:
    """Raise when semantic input-schema fields differ from the registry."""

    spec = REGISTRY_BY_NAME.get(tool_name)
    if spec is None or not _schemas_match(published, spec.input_schema):
        raise RuntimeError(f"schema mismatch: {tool_name}")


def _role_clients(settings: McpSettings, names: frozenset[str]) -> dict[str, Any]:
    needs = selection_needs(names)
    clients: dict[str, Any] = {}
    if needs.observer:
        clients["observer"] = _build_observer(settings)
    if needs.producer:
        clients["producer"] = _build_producer(settings)
    if needs.admin:
        clients["admin"] = _build_admin(settings, names)
    return clients


def _build_observer(settings: McpSettings) -> ObserverClient:
    if settings.public_base_url is None:
        raise ConfigError(_PUBLIC_BASE_URL)
    if settings.admin_base_url is None:
        raise ConfigError(_ADMIN_BASE_URL)
    if not settings.observer_token:
        raise ConfigError(_OBSERVER_TOKEN)
    return ObserverClient(
        HttpJsonTransport.from_config(origin_config(settings, settings.public_base_url)),
        bearer_token=settings.observer_token,
        admin_transport=HttpJsonTransport.from_config(
            origin_config(settings, settings.admin_base_url)
        ),
    )


def _build_producer(settings: McpSettings) -> ProducerClient:
    if settings.public_base_url is None:
        raise ConfigError(_PUBLIC_BASE_URL)
    if not settings.producer_token:
        raise ConfigError(_PRODUCER_TOKEN)
    transport = HttpJsonTransport.from_config(
        origin_config(settings, settings.public_base_url)
    )
    try:
        return ProducerClient(
            transport,
            bearer_token=settings.producer_token,
            content_key=settings.content_key,
        )
    except ValueError:
        raise ConfigError(_CONTENT_KEY) from None


def _build_admin(settings: McpSettings, names: frozenset[str]) -> AdminClient:
    if settings.admin_base_url is None:
        raise ConfigError(_ADMIN_BASE_URL)
    if not settings.admin_token:
        raise ConfigError(_ADMIN_TOKEN)
    admin_transport = HttpJsonTransport.from_config(
        origin_config(settings, settings.admin_base_url)
    )
    if "admin_get_capabilities" in names:
        if settings.public_base_url is None:
            raise ConfigError(_PUBLIC_BASE_URL)
        return AdminClient(
            HttpJsonTransport.from_config(origin_config(settings, settings.public_base_url)),
            bearer_token=settings.admin_token,
            admin_transport=admin_transport,
        )
    return AdminClient(admin_transport, bearer_token=settings.admin_token)


def _setting_for_surface(surface: str) -> str:
    if surface == "observer":
        return _OBSERVER_TOKEN
    if surface == "producer":
        return _PRODUCER_TOKEN
    if surface == "admin":
        return _ADMIN_TOKEN
    raise ConfigError(None)


def _tool_function(spec: ToolSpec, client: Any, selected: frozenset[str]) -> Any:
    if not spec.name.isidentifier():
        raise RuntimeError(f"schema mismatch: {spec.name}")
    properties = spec.input_schema.get("properties") or {}
    if not isinstance(properties, Mapping):
        properties = {}
    required = {name for name in (spec.input_schema.get("required") or []) if isinstance(name, str)}
    required_names = [name for name in properties if name in required and name.isidentifier()]
    optional_names = [name for name in properties if name not in required and name.isidentifier()]
    params: list[str] = []
    for name in required_names:
        params.append(f"{name}: {_annotation_src(name, properties[name])}")
    for name in optional_names:
        prop = properties[name]
        default = prop.get("default", None) if isinstance(prop, Mapping) else None
        params.append(f"{name}: {_annotation_src(name, prop)} = {_default_src(default)}")
    names = (*required_names, *optional_names)
    body = ", ".join(f"{name!r}: {name}" for name in names)
    source = (
        f"def {spec.name}({', '.join(params)}) -> CallToolResult:\n"
        f"    return _dispatch({{{body}}})\n"
    )

    def _dispatch(arguments: dict[str, Any]) -> CallToolResult:
        return call_tool(client, spec.name, arguments, selected=selected)

    namespace: dict[str, Any] = {
        "Annotated": Annotated,
        "Any": Any,
        "Field": Field,
        "CallToolResult": CallToolResult,
        "_dispatch": _dispatch,
    }
    exec(source, namespace)  # noqa: S102
    return namespace[spec.name]


def _annotation_src(name: str, prop: Any) -> str:
    if not isinstance(prop, Mapping):
        return "Any"
    kind = prop.get("type")
    if kind == "string" and "maxLength" in prop:
        return f"Annotated[str, Field(max_length={int(prop['maxLength'])})]"
    if kind == "integer" and ("minimum" in prop or "maximum" in prop):
        bounds: list[str] = []
        if "minimum" in prop:
            bounds.append(f"ge={int(prop['minimum'])}")
        if "maximum" in prop:
            bounds.append(f"le={int(prop['maximum'])}")
        return f"Annotated[int, Field({', '.join(bounds)})]"
    if kind == "integer":
        return "int"
    if kind == "string":
        return "str"
    if kind == "boolean":
        return "bool"
    if kind == "object":
        return "dict[str, Any]"
    if kind == "array":
        return "list[Any]"
    return "Any"


def _default_src(value: Any) -> str:
    if value is None or isinstance(value, (str, bool, int)):
        return repr(value)
    raise RuntimeError("schema mismatch: default")


def _check_published_schemas(server: MCPServer) -> None:
    for tool in server._tool_manager.list_tools():
        assert_published_schema(tool.name, tool.parameters)


def _schemas_match(published: Mapping[str, Any], expected: Mapping[str, Any]) -> bool:
    published_props = published.get("properties") or {}
    expected_props = expected.get("properties") or {}
    if not isinstance(published_props, Mapping) or set(published_props) != set(expected_props):
        return False
    for name, expected_prop in expected_props.items():
        published_prop = published_props[name]
        if not isinstance(published_prop, Mapping) or not isinstance(expected_prop, Mapping):
            return False
        if published_prop.get("type") != expected_prop.get("type"):
            return False
        if name == "limit":
            for key in ("minimum", "maximum", "default"):
                if published_prop.get(key) != expected_prop.get(key):
                    return False
        if name == "cursor" and published_prop.get("maxLength") != expected_prop.get("maxLength"):
            return False
    published_required = set(published.get("required") or [])
    expected_required = set(expected.get("required") or [])
    return published_required == expected_required


def _success_result(payload: Any) -> CallToolResult:
    text = canonical_json(payload)
    return CallToolResult(
        content=[TextContent(type="text", text=text)],
        structured_content=payload,
        is_error=False,
    )


def _error_result(exc: Exception) -> CallToolResult:
    payload = project_error(exc)
    text = canonical_json(payload)
    return CallToolResult(
        content=[TextContent(type="text", text=text)],
        structured_content=payload,
        is_error=True,
    )
