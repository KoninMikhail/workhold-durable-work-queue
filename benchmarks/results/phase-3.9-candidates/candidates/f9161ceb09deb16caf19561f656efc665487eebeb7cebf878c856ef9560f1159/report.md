# Qualification report for `cand-f9161ceb09deb16c`

Verdict: **PASS**

Validated raw digest: `5b7a4b214245dfcfcc3306d613af4882910fb6699732f0a390f86333d099fc2d`

Claims/s: 500.000 (min 500.0)
Valid success ratio: 1.000000 (min 0.999)
p99 enqueue/claim/heartbeat ns: {'enqueue': 12275979, 'claim': 12275979, 'heartbeat': 12275979}
p99 baseline complete ns: 50000000
p99 max-fanout complete ns (separate): 409000000

## Operations

- claim/baseline: valid=100 success=100 p50=12225979 p99=12275979
- complete/baseline: valid=50 success=50 p50=49975000 p99=50000000
- complete/max_fanout: valid=10 success=10 p50=404000000 p99=409000000
- enqueue/baseline: valid=50 success=50 p50=12250979 p99=12275979
- heartbeat/baseline: valid=50 success=50 p50=12250979 p99=12275979
