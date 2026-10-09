# Qualification report for `cand-0cfc016a8637efda`

Verdict: **PASS**

Validated raw digest: `dcae0f2ba2822537357a64673d94db86911082001ac6aa0176c0654b95337f31`

Claims/s: 500.000 (min 500.0)
Valid success ratio: 1.000000 (min 0.999)
p99 enqueue/claim/heartbeat ns: {'enqueue': 12342026, 'claim': 12342026, 'heartbeat': 12342026}
p99 baseline complete ns: 50000000
p99 max-fanout complete ns (separate): 409000000

## Operations

- claim/baseline: valid=100 success=100 p50=12292026 p99=12342026
- complete/baseline: valid=50 success=50 p50=49975000 p99=50000000
- complete/max_fanout: valid=10 success=10 p50=404000000 p99=409000000
- enqueue/baseline: valid=50 success=50 p50=12317026 p99=12342026
- heartbeat/baseline: valid=50 success=50 p50=12317026 p99=12342026
