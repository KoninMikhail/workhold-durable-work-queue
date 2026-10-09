"""Load bridge conformance fixtures without double-import rewrite warnings."""

pytest_plugins = [
    "tests.integration.conftest",
    "tests.conformance.bridge.fixtures",
]
