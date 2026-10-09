# Phase 3.9 storage candidate recommendation

Verdict: **PASS**

Candidates measured: 24

## Indexes

{
  "claim_p99_ns": 9838514,
  "eligible_count": 11,
  "index_bytes": 320000,
  "selected_candidate_id": "15d5e1d25b678ae15d511c4c5e64a8f93d2077e4f31ff4eee1580bb47b9b730a",
  "selected_signature": "admin_audit_log_queue_audit_idx,admin_replay_expires_at_idx,complete_replay_expires_at_idx,delivery_events_terminal_event_idx,task_attempts_task_claimed_idx,tasks_active_claim_idx,tasks_terminal_spawn_lineage_idx,tasks_terminal_task_terminal_idx",
  "tie_break": [
    "index_bytes",
    "claim_p99_ns",
    "wal_bytes",
    "candidate_id"
  ],
  "wal_bytes": 13594
}

## HASH partitions

{
  "complete_replay": {
    "claim_p99_ns": 9941816,
    "eligible_count": 5,
    "selected_candidate_id": "c35229f846f2d54a56a54b5ca8e6e459a1d9c46425c57d9c68a4b7e30338184d",
    "selected_count": 1,
    "tie_break": [
      "hash_count",
      "claim_p99_ns",
      "wal_bytes",
      "candidate_id"
    ],
    "wal_bytes": 11916
  },
  "enqueue_dedup": {
    "claim_p99_ns": 9659682,
    "eligible_count": 5,
    "selected_candidate_id": "44f139e2e27faa2730f33cb099e25843db3c91c5b05b73b3cdedc31b92faacea",
    "selected_count": 1,
    "tie_break": [
      "hash_count",
      "claim_p99_ns",
      "wal_bytes",
      "candidate_id"
    ],
    "wal_bytes": 14782
  }
}

## Payload ceiling

{
  "baseline_1kib_p99_max_ns": 20000000,
  "eligible_count": 2,
  "payload_p99_max_ns": 21000000,
  "regression_limit": 0.1,
  "selected_candidate_id": "af1f864a346052282af4f59ec9634e952d995d59a0abd843d6af9fec2ff6dfbd",
  "selected_ceiling_bytes": 1048576,
  "tie_break": [
    "payload_ceiling_bytes_desc",
    "candidate_id"
  ]
}

## Notes

- HASH count 1 is the unpartitioned control.
- Whole-request limit remains 1 MiB (1048576).
- Aggregate package is a candidate-index, not `bundle_stage: final`.
