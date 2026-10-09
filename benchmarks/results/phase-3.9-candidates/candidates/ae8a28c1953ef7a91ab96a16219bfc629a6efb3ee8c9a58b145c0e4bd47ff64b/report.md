# Qualification report for `cand-ae8a28c1953ef7a9`

Verdict: **PASS**

Validated raw digest: `3a0b2a7490c27646c1a51a0dfff8cbf7b7b2d75f7c1d09f402a80cfd9655315f`

Claims/s: 500.000 (min 500.0)
Valid success ratio: 1.000000 (min 0.999)
p99 enqueue/claim/heartbeat ns: {'enqueue': 12789985, 'claim': 12789985, 'heartbeat': 12789985}
p99 baseline complete ns: 50000000
p99 max-fanout complete ns (separate): 409000000

## Operations

- claim/baseline: valid=100 success=100 p50=12739985 p99=12789985
- complete/baseline: valid=50 success=50 p50=49975000 p99=50000000
- complete/max_fanout: valid=10 success=10 p50=404000000 p99=409000000
- enqueue/baseline: valid=50 success=50 p50=12764985 p99=12789985
- heartbeat/baseline: valid=50 success=50 p50=12764985 p99=12789985
