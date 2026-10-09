# Qualification report for `cand-6b686bee2210361f`

Verdict: **PASS**

Validated raw digest: `5b6877367559c4c48c82eb505bd6e6c711afb4270d03b9e4060d2f457fb26c70`

Claims/s: 500.000 (min 500.0)
Valid success ratio: 1.000000 (min 0.999)
p99 enqueue/claim/heartbeat ns: {'enqueue': 10505486, 'claim': 10505486, 'heartbeat': 10505486}
p99 baseline complete ns: 50000000
p99 max-fanout complete ns (separate): 409000000

## Operations

- claim/baseline: valid=100 success=100 p50=10455486 p99=10505486
- complete/baseline: valid=50 success=50 p50=49975000 p99=50000000
- complete/max_fanout: valid=10 success=10 p50=404000000 p99=409000000
- enqueue/baseline: valid=50 success=50 p50=10480486 p99=10505486
- heartbeat/baseline: valid=50 success=50 p50=10480486 p99=10505486
