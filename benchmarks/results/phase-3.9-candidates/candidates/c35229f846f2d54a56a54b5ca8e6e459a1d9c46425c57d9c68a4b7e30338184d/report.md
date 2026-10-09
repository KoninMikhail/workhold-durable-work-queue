# Qualification report for `cand-c35229f846f2d54a`

Verdict: **PASS**

Validated raw digest: `e97207febeb412eff34df7216ea47be42142c86203c2fb08816494110e1f8d70`

Claims/s: 500.000 (min 500.0)
Valid success ratio: 1.000000 (min 0.999)
p99 enqueue/claim/heartbeat ns: {'enqueue': 9941816, 'claim': 9941816, 'heartbeat': 9941816}
p99 baseline complete ns: 50000000
p99 max-fanout complete ns (separate): 409000000

## Operations

- claim/baseline: valid=100 success=100 p50=9891816 p99=9941816
- complete/baseline: valid=50 success=50 p50=49975000 p99=50000000
- complete/max_fanout: valid=10 success=10 p50=404000000 p99=409000000
- enqueue/baseline: valid=50 success=50 p50=9916816 p99=9941816
- heartbeat/baseline: valid=50 success=50 p50=9916816 p99=9941816
