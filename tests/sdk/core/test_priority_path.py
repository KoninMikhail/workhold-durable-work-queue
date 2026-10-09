"""Priority validation and path encoding primitives."""

from __future__ import annotations

import pytest

from _queue_service_client_core.priority import (
    PRIORITY_DEFAULT,
    PRIORITY_MAX,
    PRIORITY_MIN,
    validate_priority,
)
from _queue_service_client_core.transport import encode_path_segment


@pytest.mark.parametrize("value", [PRIORITY_MIN, PRIORITY_MAX, PRIORITY_DEFAULT, 42, -100])
def test_validate_priority_accepts_signed_smallint(value: int) -> None:
    assert validate_priority(value) == value
    assert type(validate_priority(value)) is int


@pytest.mark.parametrize(
    "value",
    [PRIORITY_MIN - 1, PRIORITY_MAX + 1, True, False, 1.5, "0", None, object()],
)
def test_validate_priority_rejects_invalid(value: object) -> None:
    with pytest.raises(ValueError):
        validate_priority(value)


def test_encode_path_segment_percent_encodes_reserved() -> None:
    assert encode_path_segment("a/b") == "a%2Fb"
    assert encode_path_segment("queue name") == "queue%20name"
    assert encode_path_segment("100%") == "100%25"
    assert encode_path_segment("orders") == "orders"
