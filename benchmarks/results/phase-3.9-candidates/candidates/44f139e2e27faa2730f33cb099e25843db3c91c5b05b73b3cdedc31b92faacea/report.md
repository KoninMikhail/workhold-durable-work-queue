# Qualification report for `cand-44f139e2e27faa27`

Verdict: **PASS**

Validated raw digest: `c08a5e7ce1b86dd2152dd460f347635208d209baba6421b5d22980bad9d1b9ea`

Claims/s: 500.000 (min 500.0)
Valid success ratio: 1.000000 (min 0.999)
p99 enqueue/claim/heartbeat ns: {'enqueue': 9659682, 'claim': 9659682, 'heartbeat': 9659682}
p99 baseline complete ns: 50000000
p99 max-fanout complete ns (separate): 409000000

## Operations

- claim/baseline: valid=100 success=100 p50=9609682 p99=9659682
- complete/baseline: valid=50 success=50 p50=49975000 p99=50000000
- complete/max_fanout: valid=10 success=10 p50=404000000 p99=409000000
- enqueue/baseline: valid=50 success=50 p50=9634682 p99=9659682
- heartbeat/baseline: valid=50 success=50 p50=9634682 p99=9659682
