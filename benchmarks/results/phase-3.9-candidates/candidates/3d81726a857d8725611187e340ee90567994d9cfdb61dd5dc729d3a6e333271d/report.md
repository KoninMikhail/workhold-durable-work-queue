# Qualification report for `cand-3d81726a857d8725`

Verdict: **PASS**

Validated raw digest: `f468f3c246f71f81c58ef9e3bacd63c592e10a8582fe5700248859e1415408b7`

Claims/s: 500.000 (min 500.0)
Valid success ratio: 1.000000 (min 0.999)
p99 enqueue/claim/heartbeat ns: {'enqueue': 10393610, 'claim': 10393610, 'heartbeat': 10393610}
p99 baseline complete ns: 50000000
p99 max-fanout complete ns (separate): 409000000

## Operations

- claim/baseline: valid=100 success=100 p50=10343610 p99=10393610
- complete/baseline: valid=50 success=50 p50=49975000 p99=50000000
- complete/max_fanout: valid=10 success=10 p50=404000000 p99=409000000
- enqueue/baseline: valid=50 success=50 p50=10368610 p99=10393610
- heartbeat/baseline: valid=50 success=50 p50=10368610 p99=10393610
