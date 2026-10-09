# Qualification report for `run-phase7-postgresql-18.6-001`

Verdict: **CI_SYNTHETIC_PASS**

Validated raw digest: `03542a7cc88dc3812458b657dd5c68cb3cc2e8f31c4a0f51a8d4921fbdf9785e`

Claims/s: 500.000 (min 500.0)
Valid success ratio: 1.000000 (min 0.999)
p99 enqueue/claim/heartbeat ns: {'enqueue': 2490000, 'claim': 1980000, 'heartbeat': 1045000}
p99 baseline complete ns: 5980000
p99 max-fanout complete ns (separate): 86300000

## Operations

- cancel/baseline: valid=10 success=10 p50=2000000 p99=2000000
- claim/baseline: valid=100 success=100 p50=1490000 p99=1980000
- complete/baseline: valid=50 success=50 p50=5480000 p99=5980000
- complete/max_fanout: valid=64 success=64 p50=83100000 p99=86300000
- enqueue/baseline: valid=50 success=50 p50=2240000 p99=2490000
- fail/baseline: valid=10 success=10 p50=2000000 p99=2000000
- heartbeat/baseline: valid=50 success=50 p50=920000 p99=1045000
- inspect/baseline: valid=10 success=10 p50=2000000 p99=2000000
