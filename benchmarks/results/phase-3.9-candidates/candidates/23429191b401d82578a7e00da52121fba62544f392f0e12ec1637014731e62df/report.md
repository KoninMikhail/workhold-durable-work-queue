# Qualification report for `cand-23429191b401d825`

Verdict: **PASS**

Validated raw digest: `efd29b0b18eadb9c9f1564556886d8460178bad0b4b44e46b63d6ec69eb37ae5`

Claims/s: 500.000 (min 500.0)
Valid success ratio: 1.000000 (min 0.999)
p99 enqueue/claim/heartbeat ns: {'enqueue': 10065201, 'claim': 10065201, 'heartbeat': 10065201}
p99 baseline complete ns: 50000000
p99 max-fanout complete ns (separate): 409000000

## Operations

- claim/baseline: valid=100 success=100 p50=10015201 p99=10065201
- complete/baseline: valid=50 success=50 p50=49975000 p99=50000000
- complete/max_fanout: valid=10 success=10 p50=404000000 p99=409000000
- enqueue/baseline: valid=50 success=50 p50=10040201 p99=10065201
- heartbeat/baseline: valid=50 success=50 p50=10040201 p99=10065201
