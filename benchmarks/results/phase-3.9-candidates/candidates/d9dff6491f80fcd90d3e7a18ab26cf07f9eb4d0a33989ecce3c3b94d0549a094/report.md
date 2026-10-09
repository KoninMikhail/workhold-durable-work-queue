# Qualification report for `cand-d9dff6491f80fcd9`

Verdict: **FAIL**

Validated raw digest: `fc3d5292b9e21684c6d2dd11d87614f9e1b065f78a8afd2809160b6611a97b80`

Claims/s: 500.000 (min 500.0)
Valid success ratio: 1.000000 (min 0.999)
p99 enqueue/claim/heartbeat ns: {'enqueue': 150000000, 'claim': 150000000, 'heartbeat': 150000000}
p99 baseline complete ns: 50000000
p99 max-fanout complete ns (separate): 409000000

## Operations

- claim/baseline: valid=100 success=100 p50=149950000 p99=150000000
- complete/baseline: valid=50 success=50 p50=49975000 p99=50000000
- complete/max_fanout: valid=10 success=10 p50=404000000 p99=409000000
- enqueue/baseline: valid=50 success=50 p50=149975000 p99=150000000
- heartbeat/baseline: valid=50 success=50 p50=149975000 p99=150000000
