# OpenCode fixtures

`real-store-rows.json` holds sanitized records captured from real OpenCode SQLite
databases, reflecting the exact upstream schemas and structures from official
`anomalyco/opencode` releases:
- V1: v1.19.3 (commit `e3c4eb242585dcd77be1a867391b7c25fd47ce52`)
- V2: v2.0.32 (commit `843a6c514e606b436a7fe713422ace0a93ad3d61`)

## Captured structure & schemas

The capture reflects the exact table schemas and relational layouts:
- V1: `session`, `message`, `part` tables covering user turns, assistant reasoning,
  text, tool execution (`read`), and step tokens in `step-finish`.
- V2: `session_v2`, `session_message` tables covering user turns, model switches
  (`Model.Ref` objects `{ id, providerID }`), agent switches, assistant
  reasoning/text/tools, compaction summaries, and subagent parent-child relations.

## Sanitization and transformations

To protect privacy and remove environment-specific traces while preserving exact
structural fidelity:
- All personal home paths and developer directories have been sanitized to
  generic project workspaces (`/workspace/v1-project`, `/workspace/v2-project`).
- Authentic `Model.Ref` object structures (`{ id, providerID }`) are preserved in
  V2 assistant messages and switch cards, matching upstream schema types.
- Relational foreign keys (`session_id`, `message_id`, `callID`, `parent_id`),
  timestamps, token accounting, and tool states are faithfully retained.

## Synthetic adversarial test fixtures

Synthetic adversarial edge cases (such as textless step-finish accounting,
part-level reverts on multi-part user messages, database concurrency race
conditions, and spoofed provenance lookup attempts) are generated programmatically
within `tests/opencode-provider.test.mjs` with explicit test-level assertions.
