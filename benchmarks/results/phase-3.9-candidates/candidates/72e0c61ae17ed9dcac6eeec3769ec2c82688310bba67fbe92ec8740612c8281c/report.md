# Qualification report for `cand-72e0c61ae17ed9dc`

Verdict: **PASS**

Validated raw digest: `b5512d775cfac8937051ac588252d16b74c7637957b35893eef0c1e8b3520393`

Claims/s: 500.000 (min 500.0)
Valid success ratio: 1.000000 (min 0.999)
p99 enqueue/claim/heartbeat ns: {'enqueue': 10833402, 'claim': 10833402, 'heartbeat': 10833402}
p99 baseline complete ns: 50000000
p99 max-fanout complete ns (separate): 409000000

## Operations

- claim/baseline: valid=100 success=100 p50=10783402 p99=10833402
- complete/baseline: valid=50 success=50 p50=49975000 p99=50000000
- complete/max_fanout: valid=10 success=10 p50=404000000 p99=409000000
- enqueue/baseline: valid=50 success=50 p50=10808402 p99=10833402
- heartbeat/baseline: valid=50 success=50 p50=10808402 p99=10833402
