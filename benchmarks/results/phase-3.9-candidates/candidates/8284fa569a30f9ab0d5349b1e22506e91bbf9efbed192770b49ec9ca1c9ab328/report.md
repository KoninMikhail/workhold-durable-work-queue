# Qualification report for `cand-8284fa569a30f9ab`

Verdict: **PASS**

Validated raw digest: `a5d7161b04c40426a226b63d01ee8070c96ec2002cb85c1514d4e72e784a736a`

Claims/s: 500.000 (min 500.0)
Valid success ratio: 1.000000 (min 0.999)
p99 enqueue/claim/heartbeat ns: {'enqueue': 13452918, 'claim': 13452918, 'heartbeat': 13452918}
p99 baseline complete ns: 50000000
p99 max-fanout complete ns (separate): 409000000

## Operations

- claim/baseline: valid=100 success=100 p50=13402918 p99=13452918
- complete/baseline: valid=50 success=50 p50=49975000 p99=50000000
- complete/max_fanout: valid=10 success=10 p50=404000000 p99=409000000
- enqueue/baseline: valid=50 success=50 p50=13427918 p99=13452918
- heartbeat/baseline: valid=50 success=50 p50=13427918 p99=13452918
