# OpenCode incremental adapter implementation plan

Status: approved for implementation in PR #237; implementation and verification
results will be recorded below. This document precedes the product changes.

## Contract and scope

Replace discovery's full-history payload hash with metadata discovery and
changed-session replay. Keep the public ProviderAdapter, canonical transcript,
read-only source ownership and destination transaction boundaries.

- An unchanged, established source reads **zero transcript payload rows**.
- A verified event-writer update replays only affected sessions and relationships.
- A changed session is the unit of replacement, not a message/event append.
- Legacy import's remaining message/part inserts must be found even when its
  session header and aggregate head do not change.
- Missing, unreadable, busy, unknown-schema or unstable sources retain last-good
  rows and cursors. Tombstones require a complete inventory and fresh verification.
- First import, explicit rebuild, schema/source replacement and loss of continuity
  may reconcile full payloads, with an explicit reason. Ordinary modern writes in
  a mixed store must not repeatedly scan all retained legacy payloads.
- Failed parsing/persistence does not acknowledge that unit's observation. Cursor
  and canonical replacement commit together; retry must converge after interruption.

This is metadata discovery plus session-level incrementality, **not** an
unconditional O(delta-message) guarantee. Let A = aggregate heads, S = sessions,
C = affected sessions, and B_changed = their payload bytes. Verified event-writer
work targets O(A + S + C log N + B_changed). Legacy/import compatibility may inspect
O(S + M + P) **metadata**, where M/P are retained message/part rows. Recovery may
read all payloads. No source triggers, indexes or journal changes will be installed.

## Pinned evidence and continuity constraints

- OpenCode v1.19.3: `e3c4eb242585dcd77be1a867391b7c25fd47ce52`.
- OpenCode v2.0.32: `843a6c514e606b436a7fe713422ace0a93ad3d61`.
- Starting Obelisk head: `ee9f60a57395dc2b7834ef8a8e0c2bdc50eeeb6e`;
  PR base: `a4b7329e1fcb8d907ec1f1c9543a26513d87631c`.

Normal durable writers update projector and event_sequence atomically. V2 event
payload persistence defaults off; session_message.seq is insertion order, not an
update revision. Deletion can remove the aggregate's head and log. V1 CLI import
writes session, messages and parts incrementally without one outer transaction or
aggregate-head updates. Therefore neither event payload replay nor head-only
cursors cover the declared source formats.

Primary contracts: [v1 durable transaction](https://github.com/anomalyco/opencode/blob/e3c4eb242585dcd77be1a867391b7c25fd47ce52/packages/core/src/event.ts#L239-L352),
[v1 import](https://github.com/anomalyco/opencode/blob/e3c4eb242585dcd77be1a867391b7c25fd47ce52/packages/opencode/src/cli/cmd/import.ts#L64-L109),
[v2 bus](https://github.com/anomalyco/opencode/blob/843a6c514e606b436a7fe713422ace0a93ad3d61/packages/core/src/bus.ts#L399-L430),
[v2 projector](https://github.com/anomalyco/opencode/blob/843a6c514e606b436a7fe713422ace0a93ad3d61/packages/core/src/session/projector.ts#L239-L257),
[v2 transfer](https://github.com/anomalyco/opencode/blob/843a6c514e606b436a7fe713422ace0a93ad3d61/packages/core/src/session/transfer.ts#L77-L143).
The checked-in upstream SQL fixtures contain the complete extracted schema.

A fresh connection's data_version is not a durable revision. WAL offsets/salts,
rowid tails, wall clocks and owner_id alone cannot establish continuity. Relevant
schema, store identity, complete session/header inventory, aggregate heads and
legacy member metadata form the observation. Unexplained physical source changes
must take a conservative reconciliation path, preserving existing same-millisecond
and body-only mutation tests. Concurrent arbitrary SQL edits that deliberately
hide behind another aggregate's legitimate write cannot be inferred from public
upstream metadata; do not claim a universal CDC protocol.

## Module boundaries

Keep opencode.ts as provider facade. Concentrate source schema/snapshot reads,
identity encoding/authentication, strict versioned decoding, canonical projection,
and manifest/cursor policy behind internal modules. Split only at meaningful
interfaces: a native row is decoded once for parse and raw; shared orchestration
never learns OpenCode table names or key syntax.

Identity encoders remain byte-compatible. All consumers use one decoder, including
paths containing `#ses:`. Delete metadata is an explicit union member with a native
ID, never an empty-ID sentinel. Watcher target file names and predicates compose
by union so a sibling provider's accepted files remain accepted.

## Phased commits and acceptance

1. **Plan and feasibility gate** (this document). Finalize observation/cursor and
   mixed-store recovery policy. Prefer per-unit state; introduce no fake canonical
   session or source checkpoint. If a new shared checkpoint contract becomes
   necessary, explain it before adopting it.
2. **Identity and watcher safety.** Add failing real-seam regressions for raw lookup
   and restored tombstones under delimiter-bearing paths, plus merged Hermes /
   OpenCode watcher targets. Centralize codec; authenticate destructive metadata.
3. **Decode and project.** Reject damaged legacy tool shapes without retracting
   last-good data. Decode native pre-split completed compaction and V2 lifecycle
   states consistently in parse/raw. Bump canonical marker to v3 because projection
   changes. Keep native rows for raw evidence.
4. **Incremental manifests.** Observe one read-only SQLite snapshot; indexed probes
   select modern-vs-legacy format without global message COUNTs. Compare header,
   aggregate head, legacy member sets/metadata and parent classification. Read
   payloads only for dirty sessions; validate discovery observation in parse's
   fresh snapshot. Store an explicitly versioned opaque cursor. Old/unknown cursors
   replay once; cursor serialization alone does not bump canonical marker.
5. **Compatibility and recovery.** Cover import prefixes, same-ID recreation,
   deletion races, DB/WAL rotation, schema migration, source replacement, malformed
   payload and transaction failures. Confirm app retry scheduling reaches parse
   instability; use a provider-neutral retry contract if needed. No per-provider
   orchestration branches and no source writes.
6. **Delivery.** Keep issue/PR claims aligned with the actual implementation and
   limits. Run complete Node 22 tests, root/app typecheck, lint and all Electron
   suites. Verify rebuilt CLI tarball and macOS packaged app/worker/raw entry points.
   Run fresh independent Standards and Spec adapter reviews and resolve blockers.
   Append commits to the original PR without force or merge, then add the authorized
   concise maintainer comment explaining the fixes and architecture.

Tests must fail against the preceding implementation for the intended reason.
Use synthetic data under real pinned upstream DDL, transparent SQLite row/byte
counters and real plan/persist/query/watch seams. Preserve existing assertions and
ensure new tests run in explicit CI file lists. Measure unchanged payload rows=0;
increasing unrelated history 10x must not increase dirty-session payload reads.
Record legacy metadata cost separately. Timings supplement operation counts.

## Implementation record

Pending. Record commit phases, deviations with reasons, final verification and
remaining limitations here as implementation proceeds.
