"""Alias module: plan verify references ``test_heartbeat_api``.

The Phase 3.5 heartbeat conformance suite lives in ``test_heartbeat_http.py``.
This module re-exports those tests so plan 03.6-02 verification commands resolve.
"""

from __future__ import annotations

from tests.conformance.test_heartbeat_http import *  # noqa: F403
