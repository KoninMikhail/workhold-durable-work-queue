# Qualification report for `cand-dcabc6ad820bd389`

Verdict: **PASS**

Validated raw digest: `2a6483547ca050a3e21120981077d035484fe72834fe6567fee8f3a5b8bc4e85`

Claims/s: 500.000 (min 500.0)
Valid success ratio: 1.000000 (min 0.999)
p99 enqueue/claim/heartbeat ns: {'enqueue': 10245037, 'claim': 10245037, 'heartbeat': 10245037}
p99 baseline complete ns: 50000000
p99 max-fanout complete ns (separate): 409000000

## Operations

- claim/baseline: valid=100 success=100 p50=10195037 p99=10245037
- complete/baseline: valid=50 success=50 p50=49975000 p99=50000000
- complete/max_fanout: valid=10 success=10 p50=404000000 p99=409000000
- enqueue/baseline: valid=50 success=50 p50=10220037 p99=10245037
- heartbeat/baseline: valid=50 success=50 p50=10220037 p99=10245037
