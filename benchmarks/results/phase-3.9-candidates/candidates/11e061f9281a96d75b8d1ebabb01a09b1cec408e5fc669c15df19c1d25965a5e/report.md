# Qualification report for `cand-11e061f9281a96d7`

Verdict: **PASS**

Validated raw digest: `06bbb27cefcf5d70235dd3f4caeade831097c3b9a5c8b1d4acbdc8363b6c2177`

Claims/s: 500.000 (min 500.0)
Valid success ratio: 1.000000 (min 0.999)
p99 enqueue/claim/heartbeat ns: {'enqueue': 13417817, 'claim': 13417817, 'heartbeat': 13417817}
p99 baseline complete ns: 50000000
p99 max-fanout complete ns (separate): 409000000

## Operations

- claim/baseline: valid=100 success=100 p50=13367817 p99=13417817
- complete/baseline: valid=50 success=50 p50=49975000 p99=50000000
- complete/max_fanout: valid=10 success=10 p50=404000000 p99=409000000
- enqueue/baseline: valid=50 success=50 p50=13392817 p99=13417817
- heartbeat/baseline: valid=50 success=50 p50=13392817 p99=13417817
