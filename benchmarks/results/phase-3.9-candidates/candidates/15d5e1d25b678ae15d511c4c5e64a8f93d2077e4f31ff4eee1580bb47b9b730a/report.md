# Qualification report for `cand-15d5e1d25b678ae1`

Verdict: **PASS**

Validated raw digest: `8d15e6ab0aaa53343b6418225113e5dd653f18f862f4e5c2ceb6e3c031a83436`

Claims/s: 500.000 (min 500.0)
Valid success ratio: 1.000000 (min 0.999)
p99 enqueue/claim/heartbeat ns: {'enqueue': 9838514, 'claim': 9838514, 'heartbeat': 9838514}
p99 baseline complete ns: 50000000
p99 max-fanout complete ns (separate): 409000000

## Operations

- claim/baseline: valid=100 success=100 p50=9788514 p99=9838514
- complete/baseline: valid=50 success=50 p50=49975000 p99=50000000
- complete/max_fanout: valid=10 success=10 p50=404000000 p99=409000000
- enqueue/baseline: valid=50 success=50 p50=9813514 p99=9838514
- heartbeat/baseline: valid=50 success=50 p50=9813514 p99=9838514
