"""Root pytest hooks shared across unit, contract, and integration suites."""

from __future__ import annotations

# Re-export integration DB fixtures so mixed-path collections (e.g. contracts +
# integration + top-level CLI tests) always resolve ``test_database_url``.
# Long-poll recording fixtures are shared by SDK and conformance suites.
pytest_plugins = (
    "tests.integration.conftest",
    "tests.fixtures.claim_long_poll",
)
