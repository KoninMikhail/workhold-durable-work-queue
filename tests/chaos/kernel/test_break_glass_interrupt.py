"""Wave 0 Nyquist predecessor: interrupt/chaos for delivery reclaim + elevation (D-14).

Owned by 14-05. Pre-commit kill mid delivery reclaim / mid elevation write must
leave the store consistent (no minted claim_token, elevation either durable or absent).
"""

from __future__ import annotations

from tests.chaos.kernel.harness import SCENARIO_IDS


_DELIVERY_INTERRUPT = "RC-INTERRUPT-BG-DELIVERY-RECLAIM"
_ELEVATION_INTERRUPT = "RC-INTERRUPT-BG-ELEVATION-WRITE"


def test_interrupt_mid_delivery_reclaim_pre_commit(chaos_harness) -> None:
    """Kill before commit during forceDeliveryReclaim — no partial claim_token mint."""
    assert _DELIVERY_INTERRUPT in SCENARIO_IDS
    chaos_harness.run(_DELIVERY_INTERRUPT)


def test_interrupt_mid_elevation_write_pre_commit(chaos_harness) -> None:
    """Kill before commit during durable elevation write — no sticky unlimited raise."""
    assert _ELEVATION_INTERRUPT in SCENARIO_IDS
    chaos_harness.run(_ELEVATION_INTERRUPT)
