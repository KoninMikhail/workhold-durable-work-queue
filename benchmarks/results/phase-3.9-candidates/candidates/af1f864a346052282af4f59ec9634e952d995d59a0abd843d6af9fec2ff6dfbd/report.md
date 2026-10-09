# Qualification report for `cand-af1f864a34605228`

Verdict: **PASS**

Validated raw digest: `d4122131e6d2c4fa4a420b7adc656b9dc00978023ae5aab832ff55f9c35d79f6`

Claims/s: 500.000 (min 500.0)
Valid success ratio: 1.000000 (min 0.999)
p99 enqueue/claim/heartbeat ns: {'enqueue': 21000000, 'claim': 21000000, 'heartbeat': 21000000}
p99 baseline complete ns: 50000000
p99 max-fanout complete ns (separate): 409000000

## Operations

- claim/baseline: valid=100 success=100 p50=20950000 p99=21000000
- complete/baseline: valid=50 success=50 p50=49975000 p99=50000000
- complete/max_fanout: valid=10 success=10 p50=404000000 p99=409000000
- enqueue/baseline: valid=50 success=50 p50=20975000 p99=21000000
- heartbeat/baseline: valid=50 success=50 p50=20975000 p99=21000000
