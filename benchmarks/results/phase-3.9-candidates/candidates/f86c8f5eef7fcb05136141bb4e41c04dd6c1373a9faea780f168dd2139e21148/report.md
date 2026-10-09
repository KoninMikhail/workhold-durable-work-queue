# Qualification report for `cand-f86c8f5eef7fcb05`

Verdict: **PASS**

Validated raw digest: `5bb1d5025331415091e717d4459c7a98b3f347c77ae689b9c0432736a077f0b7`

Claims/s: 500.000 (min 500.0)
Valid success ratio: 1.000000 (min 0.999)
p99 enqueue/claim/heartbeat ns: {'enqueue': 11364158, 'claim': 11364158, 'heartbeat': 11364158}
p99 baseline complete ns: 50000000
p99 max-fanout complete ns (separate): 409000000

## Operations

- claim/baseline: valid=100 success=100 p50=11314158 p99=11364158
- complete/baseline: valid=50 success=50 p50=49975000 p99=50000000
- complete/max_fanout: valid=10 success=10 p50=404000000 p99=409000000
- enqueue/baseline: valid=50 success=50 p50=11339158 p99=11364158
- heartbeat/baseline: valid=50 success=50 p50=11339158 p99=11364158
