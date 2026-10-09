# Qualification report for `cand-dc666f84f62b9b7b`

Verdict: **PASS**

Validated raw digest: `78f9c30a9fe69e296ed44e325bfc943e80ef9ca0ceb5d90509c1e910e3046184`

Claims/s: 500.000 (min 500.0)
Valid success ratio: 1.000000 (min 0.999)
p99 enqueue/claim/heartbeat ns: {'enqueue': 11200740, 'claim': 11200740, 'heartbeat': 11200740}
p99 baseline complete ns: 50000000
p99 max-fanout complete ns (separate): 409000000

## Operations

- claim/baseline: valid=100 success=100 p50=11150740 p99=11200740
- complete/baseline: valid=50 success=50 p50=49975000 p99=50000000
- complete/max_fanout: valid=10 success=10 p50=404000000 p99=409000000
- enqueue/baseline: valid=50 success=50 p50=11175740 p99=11200740
- heartbeat/baseline: valid=50 success=50 p50=11175740 p99=11200740
