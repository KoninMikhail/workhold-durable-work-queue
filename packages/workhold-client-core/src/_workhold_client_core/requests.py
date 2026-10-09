"""Pure HTTP request construction shared by sync and async transports."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from urllib.parse import quote, urlencode, urljoin

__all__ = [
    "PreparedRequest",
    "build_request_url",
    "encode_path_segment",
    "normalize_base_url",
    "prepare_json_request",
]


@dataclass(frozen=True, slots=True)
class PreparedRequest:
    """Wire-ready request produced identically for sync and async modes."""

    method: str
    url: str
    headers: dict[str, str]
    body: bytes | None
    expect_body: bool


def normalize_base_url(base_url: str) -> str:
    if not base_url:
        raise ValueError("base_url is required")
    return base_url.rstrip("/") + "/"


def encode_path_segment(value: str) -> str:
    """Percent-encode a single path segment (queue name / task id)."""

    return quote(value, safe="")


def build_request_url(
    base_url: str,
    path: str,
    *,
    query: Mapping[str, str] | None = None,
) -> str:
    if not path.startswith("/"):
        raise ValueError("path must start with /")
    url = urljoin(normalize_base_url(base_url), path.lstrip("/"))
    if query:
        encoded = urlencode(list(query.items()))
        url = f"{url}?{encoded}"
    return url


def prepare_json_request(
    base_url: str,
    method: str,
    path: str,
    *,
    headers: Mapping[str, str] | None = None,
    json_body: object | None = None,
    query: Mapping[str, str] | None = None,
    expect_body: bool = True,
) -> PreparedRequest:
    """Build one JSON request. Sync and async transports must use this helper."""

    req_headers = {"Accept": "application/json"}
    body: bytes | None = None
    if json_body is not None:
        body = json.dumps(
            json_body,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
        req_headers["Content-Type"] = "application/json; charset=utf-8"
    if headers:
        req_headers.update(headers)

    return PreparedRequest(
        method=method.upper(),
        url=build_request_url(base_url, path, query=query),
        headers=req_headers,
        body=body,
        expect_body=expect_body,
    )
