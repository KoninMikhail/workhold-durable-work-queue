# Qualification report for `run-fixture-001`

Verdict: **PASS**

Validated raw digest: `55949cef6ffdf19c9b31cc58519dbe1f948ea20cbb7455706f5ca19889ee75a3`

Claims/s: 500.000 (min 500.0)
Valid success ratio: 1.000000 (min 0.999)
p99 enqueue/claim/heartbeat ns: {'enqueue': 4450000, 'claim': 10800000, 'heartbeat': 5450000}
p99 baseline complete ns: 9900000
p99 max-fanout complete ns (separate): 409000000

## Operations

- claim/baseline: valid=100 success=100 p50=5900000 p99=10800000
- claim/pause: valid=0 success=0 p50=None p99=None
- complete/baseline: valid=50 success=50 p50=7400000 p99=9900000
- complete/max_fanout: valid=10 success=10 p50=404000000 p99=409000000
- enqueue/baseline: valid=50 success=50 p50=3200000 p99=4450000
- enqueue/invalid: valid=0 success=0 p50=None p99=None
- heartbeat/baseline: valid=50 success=50 p50=4200000 p99=5450000
