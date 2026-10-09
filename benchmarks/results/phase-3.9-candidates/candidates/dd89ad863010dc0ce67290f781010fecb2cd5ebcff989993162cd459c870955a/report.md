# Qualification report for `cand-dd89ad863010dc0c`

Verdict: **PASS**

Validated raw digest: `0ae8f4ad9ea3399eaad55a33bde194fbbeb221b3d7210a34e7d15c536efda974`

Claims/s: 500.000 (min 500.0)
Valid success ratio: 1.000000 (min 0.999)
p99 enqueue/claim/heartbeat ns: {'enqueue': 10487590, 'claim': 10487590, 'heartbeat': 10487590}
p99 baseline complete ns: 50000000
p99 max-fanout complete ns (separate): 409000000

## Operations

- claim/baseline: valid=100 success=100 p50=10437590 p99=10487590
- complete/baseline: valid=50 success=50 p50=49975000 p99=50000000
- complete/max_fanout: valid=10 success=10 p50=404000000 p99=409000000
- enqueue/baseline: valid=50 success=50 p50=10462590 p99=10487590
- heartbeat/baseline: valid=50 success=50 p50=10462590 p99=10487590
