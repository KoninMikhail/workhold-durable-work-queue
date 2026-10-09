# Qualification report for `cand-d09099ae1e2c5ad2`

Verdict: **PASS**

Validated raw digest: `b7f84e8ede2a540bce4f0c7f2dfd4b986a2ca915434e3776c0c9b8b8aaa25a91`

Claims/s: 500.000 (min 500.0)
Valid success ratio: 1.000000 (min 0.999)
p99 enqueue/claim/heartbeat ns: {'enqueue': 12437454, 'claim': 12437454, 'heartbeat': 12437454}
p99 baseline complete ns: 50000000
p99 max-fanout complete ns (separate): 409000000

## Operations

- claim/baseline: valid=100 success=100 p50=12387454 p99=12437454
- complete/baseline: valid=50 success=50 p50=49975000 p99=50000000
- complete/max_fanout: valid=10 success=10 p50=404000000 p99=409000000
- enqueue/baseline: valid=50 success=50 p50=12412454 p99=12437454
- heartbeat/baseline: valid=50 success=50 p50=12412454 p99=12437454
