# Qualification report for `cand-e0ec72bcec3968a9`

Verdict: **PASS**

Validated raw digest: `8bacd5adc751fb5c5877ef740dfeddac553b9afba944e7a77d9ac7fb05301942`

Claims/s: 500.000 (min 500.0)
Valid success ratio: 1.000000 (min 0.999)
p99 enqueue/claim/heartbeat ns: {'enqueue': 12092252, 'claim': 12092252, 'heartbeat': 12092252}
p99 baseline complete ns: 50000000
p99 max-fanout complete ns (separate): 409000000

## Operations

- claim/baseline: valid=100 success=100 p50=12042252 p99=12092252
- complete/baseline: valid=50 success=50 p50=49975000 p99=50000000
- complete/max_fanout: valid=10 success=10 p50=404000000 p99=409000000
- enqueue/baseline: valid=50 success=50 p50=12067252 p99=12092252
- heartbeat/baseline: valid=50 success=50 p50=12067252 p99=12092252
