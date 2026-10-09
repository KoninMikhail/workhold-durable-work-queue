# Qualification report for `cand-cbea11794cf866b5`

Verdict: **PASS**

Validated raw digest: `2f2b454d916a84737449ba508ae57bcd3abb02e608872d228aafdac92527967c`

Claims/s: 500.000 (min 500.0)
Valid success ratio: 1.000000 (min 0.999)
p99 enqueue/claim/heartbeat ns: {'enqueue': 12214745, 'claim': 12214745, 'heartbeat': 12214745}
p99 baseline complete ns: 50000000
p99 max-fanout complete ns (separate): 409000000

## Operations

- claim/baseline: valid=100 success=100 p50=12164745 p99=12214745
- complete/baseline: valid=50 success=50 p50=49975000 p99=50000000
- complete/max_fanout: valid=10 success=10 p50=404000000 p99=409000000
- enqueue/baseline: valid=50 success=50 p50=12189745 p99=12214745
- heartbeat/baseline: valid=50 success=50 p50=12189745 p99=12214745
