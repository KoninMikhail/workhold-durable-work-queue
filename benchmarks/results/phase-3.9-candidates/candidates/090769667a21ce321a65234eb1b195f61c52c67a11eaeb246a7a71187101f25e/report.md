# Qualification report for `cand-090769667a21ce32`

Verdict: **PASS**

Validated raw digest: `315f69060319afd01d2b5b2a4779788e919227f09dd6421ee95b187d314ebd8e`

Claims/s: 500.000 (min 500.0)
Valid success ratio: 1.000000 (min 0.999)
p99 enqueue/claim/heartbeat ns: {'enqueue': 9980678, 'claim': 9980678, 'heartbeat': 9980678}
p99 baseline complete ns: 50000000
p99 max-fanout complete ns (separate): 409000000

## Operations

- claim/baseline: valid=100 success=100 p50=9930678 p99=9980678
- complete/baseline: valid=50 success=50 p50=49975000 p99=50000000
- complete/max_fanout: valid=10 success=10 p50=404000000 p99=409000000
- enqueue/baseline: valid=50 success=50 p50=9955678 p99=9980678
- heartbeat/baseline: valid=50 success=50 p50=9955678 p99=9980678
