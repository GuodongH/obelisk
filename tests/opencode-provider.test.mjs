// Copyright (C) 2026 tommy0103 and contributors.
// SPDX-License-Identifier: AGPL-3.0-only

import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  mkdirSync,
  readFileSync,
  renameSync,
  writeFileSync,
} from 'node:fs';
import { join, normalize } from 'node:path';
import { DatabaseSync } from 'node:sqlite';

import { persist } from '../packages/core/src/persist.ts';
import {
  createProviderIndexPlan,
  indexProviderPlan,
} from '../packages/core/src/provider-indexing.ts';
import { runWriteTransaction } from '../packages/core/src/tx.ts';
import {
  createOpencodeProvider,
  defaultOpencodeDataRoot,
  isValidChannelDbName,
  opencodeSessionId,
  opencodeStoreHash,
  opencodeUnitKey,
} from '../packages/core/src/providers/opencode.ts';
import { openOpencodeDatabaseWithNodeSqlite } from '../packages/core/src/providers/opencode-node.ts';
import { createProviderRegistry } from '../packages/core/src/providers/registry.ts';
import { assembleSessionDetail } from '../packages/core/src/session-detail.ts';
import { makeTempDir } from './temp-dirs.mjs';

const SCHEMA = readFileSync(new URL('../packages/core/src/schema.sql', import.meta.url), 'utf8');

const OPENCODE_DDL = `
CREATE TABLE IF NOT EXISTS session (
  id text PRIMARY KEY,
  project_id text NOT NULL,
  workspace_id text,
  parent_id text,
  slug text DEFAULT '' NOT NULL,
  directory text NOT NULL,
  path text,
  title text DEFAULT '' NOT NULL,
  version text NOT NULL,
  share_url text,
  summary_additions integer,
  summary_deletions integer,
  summary_files integer,
  summary_diffs text,
  metadata text,
  cost real DEFAULT 0 NOT NULL,
  tokens_input integer DEFAULT 0 NOT NULL,
  tokens_output integer DEFAULT 0 NOT NULL,
  tokens_reasoning integer DEFAULT 0 NOT NULL,
  tokens_cache_read integer DEFAULT 0 NOT NULL,
  tokens_cache_write integer DEFAULT 0 NOT NULL,
  revert text,
  permission text,
  agent text,
  model text,
  time_created integer NOT NULL,
  time_updated integer NOT NULL,
  time_compacting integer,
  time_archived integer
);

CREATE TABLE IF NOT EXISTS message (
  id text PRIMARY KEY,
  session_id text NOT NULL,
  time_created integer NOT NULL,
  time_updated integer NOT NULL,
  data text NOT NULL
);

CREATE TABLE IF NOT EXISTS part (
  id text PRIMARY KEY,
  message_id text NOT NULL,
  session_id text NOT NULL,
  time_created integer NOT NULL,
  time_updated integer NOT NULL,
  data text NOT NULL
);

CREATE TABLE IF NOT EXISTS session_v2 (
  id text PRIMARY KEY,
  project_id text NOT NULL,
  workspace_id text,
  parent_id text,
  fork_session_id text,
  fork_boundary text,
  slug text DEFAULT '' NOT NULL,
  directory text NOT NULL,
  path text,
  title text,
  version text NOT NULL,
  share_url text,
  summary_additions integer,
  summary_deletions integer,
  summary_files integer,
  summary_diffs text,
  metadata text,
  cost real DEFAULT 0 NOT NULL,
  tokens_input integer DEFAULT 0 NOT NULL,
  tokens_output integer DEFAULT 0 NOT NULL,
  tokens_reasoning integer DEFAULT 0 NOT NULL,
  tokens_cache_read integer DEFAULT 0 NOT NULL,
  tokens_cache_write integer DEFAULT 0 NOT NULL,
  revert text,
  permission text,
  agent text,
  model text,
  time_created integer NOT NULL,
  time_updated integer NOT NULL,
  time_compacting integer,
  time_archived integer,
  time_suspended integer,
  resume_attempts integer DEFAULT 0 NOT NULL,
  time_idle integer,
  time_viewed integer,
  idle_outcome text
);

CREATE TABLE IF NOT EXISTS session_message (
  id text PRIMARY KEY,
  session_id text NOT NULL,
  type text NOT NULL,
  seq integer NOT NULL,
  time_created integer NOT NULL,
  time_updated integer NOT NULL,
  data text NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS session_message_session_seq_idx ON session_message (session_id, seq);
CREATE INDEX IF NOT EXISTS session_message_session_time_created_id_idx ON session_message (session_id, time_created, id);
CREATE INDEX IF NOT EXISTS message_session_time_created_id_idx ON message (session_id, time_created, id);
CREATE INDEX IF NOT EXISTS part_message_id_id_idx ON part (message_id, id);
`;

function openDb(path) {
  return openOpencodeDatabaseWithNodeSqlite(path);
}

function initTestStore(dbPath) {
  const db = new DatabaseSync(dbPath);
  db.exec(OPENCODE_DDL);
  db.close();
}

function drain(generator) {
  const values = [];
  let step = generator.next();
  while (!step.done) {
    values.push(step.value);
    step = generator.next();
  }
  return { values, cursor: step.value };
}

function discover(provider, { cursors = new Map(), indexed = [], issues = [], changedPaths } = {}) {
  return provider.discover({
    lastCursor: key => cursors.get(key) ?? null,
    indexedSessions: () => indexed,
    reportIncompleteInventory: issue => issues.push(issue),
    ...(changedPaths === undefined ? {} : { changedPaths }),
  });
}

function createMemoryIndex() {
  const db = new DatabaseSync(':memory:');
  db.exec(SCHEMA);
  return db;
}

test('discover() respects default XDG data root, custom root, and OPENCODE_DB', () => {
  const home = makeTempDir('opencode-discover-root-');
  const customXdg = join(home, 'xdg-data');
  const prevXdg = process.env.XDG_DATA_HOME;
  const prevOpencodeDb = process.env.OPENCODE_DB;

  try {
    delete process.env.XDG_DATA_HOME;
    delete process.env.OPENCODE_DB;

    // 1. Default directory
    const defaultRoot = defaultOpencodeDataRoot(home);
    assert.equal(defaultRoot, join(home, '.local', 'share', 'opencode'));

    // 2. XDG_DATA_HOME
    process.env.XDG_DATA_HOME = customXdg;
    assert.equal(defaultOpencodeDataRoot(home), join(customXdg, 'opencode'));

    // 3. OPENCODE_DB=:memory: produces explicit diagnostic
    process.env.OPENCODE_DB = ':memory:';
    const provider = createOpencodeProvider({ homeDir: home, openDatabase: openDb });
    const issues = [];
    const units = discover(provider, { issues });
    assert.deepEqual(units, []);
    assert.equal(issues.length, 1);
    assert.match(issues[0].error, /:memory:/i);

    // 4. OPENCODE_DB relative path
    const storeDir = join(customXdg, 'opencode');
    mkdirSync(storeDir, { recursive: true });
    const relDbPath = join(storeDir, 'custom.db');
    initTestStore(relDbPath);
    process.env.OPENCODE_DB = 'custom.db';
    const relProvider = createOpencodeProvider({ homeDir: home, openDatabase: openDb });
    const relUnits = discover(relProvider);
    assert.equal(relUnits.length, 0); // Empty tables, valid census

    // 5. Explicit rootDir overrides OPENCODE_DB
    const explicitDir = join(home, 'explicit-root');
    mkdirSync(explicitDir, { recursive: true });
    const explicitDb = join(explicitDir, 'opencode.db');
    initTestStore(explicitDb);
    const expProvider = createOpencodeProvider({ rootDir: explicitDir, homeDir: home, openDatabase: openDb });
    const expUnits = discover(expProvider);
    assert.equal(expUnits.length, 0);
  } finally {
    if (prevXdg !== undefined) process.env.XDG_DATA_HOME = prevXdg; else delete process.env.XDG_DATA_HOME;
    if (prevOpencodeDb !== undefined) process.env.OPENCODE_DB = prevOpencodeDb; else delete process.env.OPENCODE_DB;
  }
});

test('channel DB discovery strictly matches upstream channel naming rule', () => {
  assert.equal(isValidChannelDbName('opencode.db'), false);
  assert.equal(isValidChannelDbName('opencode-dev.db'), true);
  assert.equal(isValidChannelDbName('opencode-next.db'), true);
  assert.equal(isValidChannelDbName('opencode-pr_123.db'), true);
  assert.equal(isValidChannelDbName('opencode-invalid/name.db'), false);
  assert.equal(isValidChannelDbName('arbitrary.db'), false);
  assert.equal(isValidChannelDbName('opencode.sqlite'), false);
});

test('discover() census handles V1-only, V2-only, mixed overlap, and migration', () => {
  const home = makeTempDir('opencode-census-');
  const dir = join(home, '.local', 'share', 'opencode');
  mkdirSync(dir, { recursive: true });
  const dbPath = join(dir, 'opencode.db');
  initTestStore(dbPath);

  const db = new DatabaseSync(dbPath);
  // V1 only session: ses_v1_only
  db.prepare(`
    INSERT INTO session (id, project_id, directory, title, version, time_created, time_updated)
    VALUES (?, ?, ?, ?, ?, ?, ?)
  `).run('ses_v1_only', 'proj1', '/work/proj1', 'V1 Session', '1.19.3', 1000, 1000);
  db.prepare(`
    INSERT INTO message (id, session_id, time_created, time_updated, data)
    VALUES (?, ?, ?, ?, ?)
  `).run('msg_v1_1', 'ses_v1_only', 1000, 1000, JSON.stringify({ role: 'user', text: 'hello' }));

  // V2 only session: ses_v2_only
  db.prepare(`
    INSERT INTO session_v2 (id, project_id, directory, title, version, time_created, time_updated)
    VALUES (?, ?, ?, ?, ?, ?, ?)
  `).run('ses_v2_only', 'proj1', '/work/proj1', 'V2 Session', '2.0.32', 2000, 2000);
  db.prepare(`
    INSERT INTO session_message (id, session_id, type, seq, time_created, time_updated, data)
    VALUES (?, ?, ?, ?, ?, ?, ?)
  `).run('msg_v2_1', 'ses_v2_only', 'user', 1, 2000, 2000, JSON.stringify({ text: 'hello v2' }));

  // Overlapping session in both tables: ses_overlap (V2 must be preferred)
  db.prepare(`
    INSERT INTO session (id, project_id, directory, title, version, time_created, time_updated)
    VALUES (?, ?, ?, ?, ?, ?, ?)
  `).run('ses_overlap', 'proj1', '/work/proj1', 'Overlap Old V1', '1.19.3', 3000, 3000);
  db.prepare(`
    INSERT INTO session_v2 (id, project_id, directory, title, version, time_created, time_updated)
    VALUES (?, ?, ?, ?, ?, ?, ?)
  `).run('ses_overlap', 'proj1', '/work/proj1', 'Overlap Fresh V2', '2.0.32', 3000, 3500);
  db.prepare(`
    INSERT INTO session_message (id, session_id, type, seq, time_created, time_updated, data)
    VALUES (?, ?, ?, ?, ?, ?, ?)
  `).run('msg_v2_overlap_1', 'ses_overlap', 'user', 1, 3500, 3500, JSON.stringify({ text: 'v2 overlap msg' }));
  db.close();

  const provider = createOpencodeProvider({ rootDir: dir, homeDir: home, openDatabase: openDb });
  const units = discover(provider);

  assert.equal(units.length, 3);
  const v1Unit = units.find(u => u.sessionId.includes('ses_v1_only'));
  const v2Unit = units.find(u => u.sessionId.includes('ses_v2_only'));
  const overlapUnit = units.find(u => u.sessionId.includes('ses_overlap'));

  assert.ok(v1Unit && v1Unit.meta.format === 'v1');
  assert.ok(v2Unit && v2Unit.meta.format === 'v2');
  assert.ok(overlapUnit && overlapUnit.meta.format === 'v2', 'overlapping session prefers V2');

  // Parse overlapUnit and check it emitted V2 records
  const { values } = drain(provider.parse(overlapUnit, null));
  const sessionRecord = values.find(r => r.kind === 'session');
  assert.equal(sessionRecord.title, 'Overlap Fresh V2');
  assert.equal(sessionRecord.version, '2.0.32');
});

test('composite session ID prevents cross-store collision with identical native IDs', () => {
  const home = makeTempDir('opencode-multistore-');
  const storeA = join(home, 'storeA.db');
  const storeB = join(home, 'storeB.db');
  initTestStore(storeA);
  initTestStore(storeB);

  const nativeId = 'ses_same_id_123';
  const idA = opencodeSessionId(nativeId, storeA);
  const idB = opencodeSessionId(nativeId, storeB);

  assert.notEqual(idA, idB, 'different store paths yield distinct composite session IDs');
  assert.equal(idA.startsWith('opencode:ses_same_id_123:'), true);
  assert.equal(idB.startsWith('opencode:ses_same_id_123:'), true);
  assert.equal(idA.slice(-64), opencodeStoreHash(storeA));
  assert.equal(idB.slice(-64), opencodeStoreHash(storeB));
});

test('parse() projects V2 fork sessions reading own rows without ancestor recursion', () => {
  const home = makeTempDir('opencode-fork-');
  const dir = join(home, 'data');
  mkdirSync(dir, { recursive: true });
  const dbPath = join(dir, 'opencode.db');
  initTestStore(dbPath);

  const db = new DatabaseSync(dbPath);
  db.prepare(`
    INSERT INTO session_v2 (id, project_id, fork_session_id, directory, title, version, time_created, time_updated)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
  `).run('ses_forked', 'proj1', 'ses_root_ancestor', '/work/proj1', 'Forked Session', '2.0.32', 5000, 5000);

  // Forked session has its own materialized messages
  db.prepare(`
    INSERT INTO session_message (id, session_id, type, seq, time_created, time_updated, data)
    VALUES (?, ?, ?, ?, ?, ?, ?)
  `).run('msg_f1', 'ses_forked', 'user', 1, 5000, 5000, JSON.stringify({ text: 'materialized in fork' }));
  db.close();

  const provider = createOpencodeProvider({ rootDir: dir, homeDir: home, openDatabase: openDb });
  const units = discover(provider);
  assert.equal(units.length, 1);

  const { values } = drain(provider.parse(units[0], null));
  const msgs = values.filter(r => r.kind === 'message');
  assert.equal(msgs.length, 1);
  assert.equal(msgs[0].text, 'materialized in fork');
});

test('parse() recovers subagent parent relationships and does not guess toolcallID', () => {
  const home = makeTempDir('opencode-subagent-');
  const dir = join(home, 'data');
  mkdirSync(dir, { recursive: true });
  const dbPath = join(dir, 'opencode.db');
  initTestStore(dbPath);

  const db = new DatabaseSync(dbPath);
  db.prepare(`
    INSERT INTO session_v2 (id, project_id, parent_id, directory, title, version, time_created, time_updated, agent)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
  `).run('ses_parent', 'proj1', null, '/work/proj1', 'Parent Session', '2.0.32', 1000, 1000, null);
  db.prepare(`
    INSERT INTO session_message (id, session_id, type, seq, time_created, time_updated, data)
    VALUES (?, ?, ?, ?, ?, ?, ?)
  `).run('msg_p1', 'ses_parent', 'user', 1, 1000, 1000, JSON.stringify({ text: 'spawn agent' }));

  db.prepare(`
    INSERT INTO session_v2 (id, project_id, parent_id, directory, title, version, time_created, time_updated, agent)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
  `).run('ses_child', 'proj1', 'ses_parent', '/work/proj1', 'Explore Subagent', '2.0.32', 1050, 1050, 'explore');
  db.prepare(`
    INSERT INTO session_message (id, session_id, type, seq, time_created, time_updated, data)
    VALUES (?, ?, ?, ?, ?, ?, ?)
  `).run('msg_c1', 'ses_child', 'user', 1, 1050, 1050, JSON.stringify({ text: 'subagent prompt' }));
  db.close();

  const provider = createOpencodeProvider({ rootDir: dir, homeDir: home, openDatabase: openDb });
  const units = discover(provider);

  const parentUnit = units.find(u => u.sessionId.includes('ses_parent'));
  const childUnit = units.find(u => u.sessionId.includes('ses_child'));

  assert.equal(parentUnit.isSubagent, false);
  assert.equal(childUnit.isSubagent, true);
  assert.equal(childUnit.agentId, childUnit.sessionId);

  const { values } = drain(provider.parse(childUnit, null));
  const subagentRecord = values.find(r => r.kind === 'subagent');
  assert.ok(subagentRecord, 'child emits SubagentRecord');
  assert.equal(subagentRecord.agent_id, childUnit.sessionId);
  assert.equal(subagentRecord.session_id, parentUnit.sessionId);
  assert.equal(subagentRecord.parent_tool_use_id, null, 'does not guess toolcallID');
  assert.equal(subagentRecord.agent_type, 'explore');

  const childMsg = values.find(r => r.kind === 'message');
  assert.equal(childMsg.is_sidechain, 1);
  assert.equal(childMsg.agent_id, childUnit.sessionId);
});

test('parse() handles tool lifecycle (running, completed, error, file_path) and block order', () => {
  const home = makeTempDir('opencode-tools-');
  const dir = join(home, 'data');
  mkdirSync(dir, { recursive: true });
  const dbPath = join(dir, 'opencode.db');
  initTestStore(dbPath);

  const db = new DatabaseSync(dbPath);
  db.prepare(`
    INSERT INTO session_v2 (id, project_id, directory, title, version, time_created, time_updated)
    VALUES (?, ?, ?, ?, ?, ?, ?)
  `).run('ses_tools', 'proj1', '/work/proj1', 'Tool Session', '2.0.32', 1000, 1000);

  // Assistant message with reasoning, completed tool, running tool, error tool, and text
  db.prepare(`
    INSERT INTO session_message (id, session_id, type, seq, time_created, time_updated, data)
    VALUES (?, ?, ?, ?, ?, ?, ?)
  `).run('msg_asst_tools', 'ses_tools', 'assistant', 1, 1000, 1000, JSON.stringify({
    content: [
      { type: 'reasoning', text: 'Step 1: thinking' },
      {
        type: 'tool',
        id: 'call_edit_1',
        name: 'edit',
        state: {
          status: 'completed',
          input: { path: 'packages/core/src/index.ts', oldString: 'a', newString: 'b' },
          content: 'Applied patch'
        }
      },
      {
        type: 'tool',
        id: 'call_bash_running',
        name: 'bash',
        state: {
          status: 'running',
          input: { command: 'cargo build' }
        }
      },
      {
        type: 'tool',
        id: 'call_read_error',
        name: 'read',
        state: {
          status: 'error',
          input: { path: 'nonexistent.txt' },
          error: 'File not found'
        }
      },
      { type: 'text', text: 'All operations finished.' }
    ]
  }));
  db.close();

  const provider = createOpencodeProvider({ rootDir: dir, homeDir: home, openDatabase: openDb });
  const units = discover(provider);
  const { values } = drain(provider.parse(units[0], null));

  const calls = values.filter(r => r.kind === 'tool_call');
  const results = values.filter(r => r.kind === 'tool_result');

  assert.equal(calls.length, 3);
  assert.equal(results.length, 2, 'running tool does not fake a result');

  const editCall = calls.find(c => c.name === 'edit');
  const editResult = results.find(r => r.tool_use_id === editCall.id);
  assert.equal(editCall.file_path, 'packages/core/src/index.ts');
  assert.equal(editResult.is_error, 0);
  assert.equal(editResult.content, 'Applied patch');

  const errorCall = calls.find(c => c.name === 'read');
  const errorResult = results.find(r => r.tool_use_id === errorCall.id);
  assert.equal(errorCall.file_path, 'nonexistent.txt');
  assert.equal(errorResult.is_error, 1);
  assert.equal(errorResult.content, 'File not found');

  // Verify multi-block sequence and parent chaining
  const msgs = values.filter(r => r.kind === 'message');
  assert.equal(msgs[0].content_type, 'thinking');
  assert.equal(msgs[1].content_type, 'tool_use');
  assert.equal(msgs[2].content_type, 'tool_use');
  assert.equal(msgs[3].content_type, 'tool_use');
  assert.equal(msgs[4].content_type, 'text');

  assert.equal(msgs[0].parent_uuid, null);
  assert.equal(msgs[1].parent_uuid, msgs[0].uuid);
  assert.equal(msgs[2].parent_uuid, msgs[1].uuid);
  assert.equal(msgs[3].parent_uuid, msgs[2].uuid);
  assert.equal(msgs[4].parent_uuid, msgs[3].uuid);
});

test('canonical token accounting: V2 input+cache, output+reasoning; V1 step-finish priority and partial-step', () => {
  const home = makeTempDir('opencode-tokens-');
  const dir = join(home, 'data');
  mkdirSync(dir, { recursive: true });
  const dbPath = join(dir, 'opencode.db');
  initTestStore(dbPath);

  const db = new DatabaseSync(dbPath);
  // V2 session with cache read/write and reasoning tokens
  db.prepare(`
    INSERT INTO session_v2 (id, project_id, directory, title, version, time_created, time_updated)
    VALUES (?, ?, ?, ?, ?, ?, ?)
  `).run('ses_v2_tokens', 'proj1', '/work/proj1', 'V2 Tokens', '2.0.32', 1000, 1000);
  db.prepare(`
    INSERT INTO session_message (id, session_id, type, seq, time_created, time_updated, data)
    VALUES (?, ?, ?, ?, ?, ?, ?)
  `).run('msg_v2_asst', 'ses_v2_tokens', 'assistant', 1, 1000, 1000, JSON.stringify({
    content: [{ type: 'text', text: 'response' }],
    tokens: {
      input: 100,
      output: 50,
      reasoning: 25,
      cache: { read: 300, write: 40 }
    }
  }));

  // V1 session with multiple step-finish parts
  db.prepare(`
    INSERT INTO session (id, project_id, directory, title, version, time_created, time_updated)
    VALUES (?, ?, ?, ?, ?, ?, ?)
  `).run('ses_v1_tokens', 'proj1', '/work/proj1', 'V1 Tokens', '1.19.3', 2000, 2000);
  db.prepare(`
    INSERT INTO message (id, session_id, time_created, time_updated, data)
    VALUES (?, ?, ?, ?, ?)
  `).run('msg_v1_asst', 'ses_v1_tokens', 2000, 2000, JSON.stringify({
    role: 'assistant',
    tokens: { input: 9999, output: 9999 } // Cumulative, must NOT be duplicated when step-finish exists
  }));
  db.prepare(`
    INSERT INTO part (id, message_id, session_id, time_created, time_updated, data)
    VALUES (?, ?, ?, ?, ?, ?)
  `).run('p_s1_1_start', 'msg_v1_asst', 'ses_v1_tokens', 2000, 2000, JSON.stringify({ type: 'step-start' }));
  db.prepare(`
    INSERT INTO part (id, message_id, session_id, time_created, time_updated, data)
    VALUES (?, ?, ?, ?, ?, ?)
  `).run('p_s1_2_text', 'msg_v1_asst', 'ses_v1_tokens', 2001, 2001, JSON.stringify({ type: 'text', text: 'step 1' }));
  db.prepare(`
    INSERT INTO part (id, message_id, session_id, time_created, time_updated, data)
    VALUES (?, ?, ?, ?, ?, ?)
  `).run('p_s1_3_finish', 'msg_v1_asst', 'ses_v1_tokens', 2002, 2002, JSON.stringify({
    type: 'step-finish',
    tokens: { input: 10, output: 5, reasoning: 0, cache: { read: 0, write: 0 } }
  }));
  db.prepare(`
    INSERT INTO part (id, message_id, session_id, time_created, time_updated, data)
    VALUES (?, ?, ?, ?, ?, ?)
  `).run('p_s2_1_start', 'msg_v1_asst', 'ses_v1_tokens', 2003, 2003, JSON.stringify({ type: 'step-start' }));
  db.prepare(`
    INSERT INTO part (id, message_id, session_id, time_created, time_updated, data)
    VALUES (?, ?, ?, ?, ?, ?)
  `).run('p_s2_2_text', 'msg_v1_asst', 'ses_v1_tokens', 2004, 2004, JSON.stringify({ type: 'text', text: 'step 2' }));
  db.prepare(`
    INSERT INTO part (id, message_id, session_id, time_created, time_updated, data)
    VALUES (?, ?, ?, ?, ?, ?)
  `).run('p_s2_3_finish', 'msg_v1_asst', 'ses_v1_tokens', 2005, 2005, JSON.stringify({
    type: 'step-finish',
    tokens: { input: 20, output: 8, reasoning: 2, cache: { read: 50, write: 5 } }
  }));
  db.close();

  const provider = createOpencodeProvider({ rootDir: dir, homeDir: home, openDatabase: openDb });
  const units = discover(provider);

  const uV2 = units.find(u => u.sessionId.includes('ses_v2_tokens'));
  const { values: v2Vals } = drain(provider.parse(uV2, null));
  const v2Msg = v2Vals.find(r => r.kind === 'message');
  assert.equal(v2Msg.input_tokens, 100 + 300 + 40, 'input + cache.read + cache.write');
  assert.equal(v2Msg.output_tokens, 50 + 25, 'output + reasoning');

  const uV1 = units.find(u => u.sessionId.includes('ses_v1_tokens'));
  const { values: v1Vals } = drain(provider.parse(uV1, null));
  const v1Msgs = v1Vals.filter(r => r.kind === 'message');
  assert.equal(v1Msgs.length, 2);
  assert.equal(v1Msgs[0].input_tokens, 10);
  assert.equal(v1Msgs[0].output_tokens, 5);
  assert.equal(v1Msgs[1].input_tokens, 20 + 50 + 5);
  assert.equal(v1Msgs[1].output_tokens, 8 + 2);
});

test('parse() handles V1 revert boundary and part-level undo', () => {
  const home = makeTempDir('opencode-revert-');
  const dir = join(home, 'data');
  mkdirSync(dir, { recursive: true });
  const dbPath = join(dir, 'opencode.db');
  initTestStore(dbPath);

  const db = new DatabaseSync(dbPath);
  db.prepare(`
    INSERT INTO session (id, project_id, directory, title, version, time_created, time_updated, revert)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
  `).run('ses_revert', 'proj1', '/work/proj1', 'Revert Session', '1.19.3', 1000, 1000, JSON.stringify({
    messageID: 'msg_2',
    partID: 'part_2_2_cut'
  }));

  db.prepare(`INSERT INTO message (id, session_id, time_created, time_updated, data) VALUES (?, ?, ?, ?, ?)`).run(
    'msg_1', 'ses_revert', 1000, 1000, JSON.stringify({ role: 'user', text: 'turn 1' }),
  );
  db.prepare(`INSERT INTO message (id, session_id, time_created, time_updated, data) VALUES (?, ?, ?, ?, ?)`).run(
    'msg_2', 'ses_revert', 1010, 1010, JSON.stringify({ role: 'assistant' }),
  );
  db.prepare(`INSERT INTO part (id, message_id, session_id, time_created, time_updated, data) VALUES (?, ?, ?, ?, ?, ?)`).run(
    'part_2_1_kept', 'msg_2', 'ses_revert', 1010, 1010, JSON.stringify({ type: 'text', text: 'part kept' }),
  );
  db.prepare(`INSERT INTO part (id, message_id, session_id, time_created, time_updated, data) VALUES (?, ?, ?, ?, ?, ?)`).run(
    'part_2_2_cut', 'msg_2', 'ses_revert', 1011, 1011, JSON.stringify({ type: 'text', text: 'part cut' }),
  );
  db.prepare(`INSERT INTO message (id, session_id, time_created, time_updated, data) VALUES (?, ?, ?, ?, ?)`).run(
    'msg_3', 'ses_revert', 1020, 1020, JSON.stringify({ role: 'user', text: 'turn 3' }),
  );
  db.close();

  const provider = createOpencodeProvider({ rootDir: dir, homeDir: home, openDatabase: openDb });
  const units = discover(provider);
  const { values } = drain(provider.parse(units[0], null));

  const msgs = values.filter(r => r.kind === 'message');
  assert.equal(msgs[0].visibility, 'visible'); // msg_1
  assert.equal(msgs[1].visibility, 'visible'); // part_2_kept
  assert.equal(msgs[2].visibility, 'inactive'); // part_2_cut
  assert.equal(msgs[3].visibility, 'inactive'); // msg_3
});

test('parse() emits textless message records with unknown or tool_use content_type', () => {
  const home = makeTempDir('opencode-textless-');
  const dir = join(home, 'data');
  mkdirSync(dir, { recursive: true });
  const dbPath = join(dir, 'opencode.db');
  initTestStore(dbPath);

  const db = new DatabaseSync(dbPath);
  db.prepare(`
    INSERT INTO session_v2 (id, project_id, directory, title, version, time_created, time_updated)
    VALUES (?, ?, ?, ?, ?, ?, ?)
  `).run('ses_textless', 'proj1', '/work/proj1', 'Textless', '2.0.32', 1000, 1000);
  db.prepare(`
    INSERT INTO session_message (id, session_id, type, seq, time_created, time_updated, data)
    VALUES (?, ?, ?, ?, ?, ?, ?)
  `).run('msg_tl', 'ses_textless', 'assistant', 1, 1000, 1000, JSON.stringify({
    content: [],
    tokens: { input: 50, output: 0 }
  }));
  db.close();

  const provider = createOpencodeProvider({ rootDir: dir, homeDir: home, openDatabase: openDb });
  const units = discover(provider);
  const { values } = drain(provider.parse(units[0], null));

  const msg = values.find(r => r.kind === 'message');
  assert.ok(msg);
  assert.equal(msg.text, null);
  assert.equal(msg.content_type, 'unknown');
  assert.equal(msg.input_tokens, 50);
});

test('raw() validates store identity, prevents cross-session leaks, and supports messageText', () => {
  const home = makeTempDir('opencode-raw-');
  const dir = join(home, 'data');
  mkdirSync(dir, { recursive: true });
  const dbPath = join(dir, 'opencode.db');
  initTestStore(dbPath);

  const db = new DatabaseSync(dbPath);
  db.prepare(`
    INSERT INTO session_v2 (id, project_id, directory, title, version, time_created, time_updated)
    VALUES (?, ?, ?, ?, ?, ?, ?)
  `).run('ses_target', 'proj1', '/work/proj1', 'Target', '2.0.32', 1000, 1000);
  db.prepare(`
    INSERT INTO session_message (id, session_id, type, seq, time_created, time_updated, data)
    VALUES (?, ?, ?, ?, ?, ?, ?)
  `).run('msg_target_1', 'ses_target', 'user', 1, 1000, 1000, JSON.stringify({ text: 'Secret target message' }));

  db.prepare(`
    INSERT INTO session_v2 (id, project_id, directory, title, version, time_created, time_updated)
    VALUES (?, ?, ?, ?, ?, ?, ?)
  `).run('ses_other', 'proj1', '/work/proj1', 'Other', '2.0.32', 2000, 2000);
  db.close();

  const provider = createOpencodeProvider({ rootDir: dir, homeDir: home, openDatabase: openDb });
  const sessionId = opencodeSessionId('ses_target', dbPath);
  const jsonlPath = opencodeUnitKey(dbPath, 'ses_target');
  const uuid = `${sessionId}:m:000001:msg_target_1:msg`;

  // 1. Valid lookup
  const rawOk = provider.raw({
    source: 'opencode',
    messageUuid: uuid,
    session: { id: sessionId, jsonl_path: jsonlPath },
    agentId: null,
  });
  assert.ok(rawOk);
  assert.equal(rawOk.messageText, 'Secret target message');
  assert.ok(rawOk.text.includes('Secret target message'));

  // 2. Spoofed store identity
  const fakeSessionId = 'opencode:ses_target:0000000000000000000000000000000000000000000000000000000000000000';
  const rawSpoof = provider.raw({
    source: 'opencode',
    messageUuid: uuid,
    session: { id: fakeSessionId, jsonl_path: jsonlPath },
    agentId: null,
  });
  assert.equal(rawSpoof, null, 'rejects mismatched store identity');

  // 3. Cross-session lookup attempt (requesting target message from session_other)
  const otherSessionId = opencodeSessionId('ses_other', dbPath);
  const otherJsonlPath = opencodeUnitKey(dbPath, 'ses_other');
  const rawCross = provider.raw({
    source: 'opencode',
    messageUuid: uuid,
    session: { id: otherSessionId, jsonl_path: otherJsonlPath },
    agentId: null,
  });
  assert.equal(rawCross, null, 'cannot leak messages across sessions');

  // 4. Cursor mismatch: valid session/message but stale/invalid cursor passed
  const rawCursorMismatch = provider.raw({
    source: 'opencode',
    messageUuid: uuid,
    session: { id: sessionId, jsonl_path: jsonlPath },
    cursor: '999999:0:v1:000000000000000000000000',
    agentId: null,
  });
  assert.equal(rawCursorMismatch, null, 'raw returns null when cursor mismatches source database state');
});

test('ADR-0007 canonical transcript round-trip equals SQLite persistence assembly', () => {
  const home = makeTempDir('opencode-adr0007-');
  const dir = join(home, 'data');
  mkdirSync(dir, { recursive: true });
  const dbPath = join(dir, 'opencode.db');
  initTestStore(dbPath);

  const db = new DatabaseSync(dbPath);
  db.prepare(`
    INSERT INTO session_v2 (id, project_id, directory, title, version, time_created, time_updated)
    VALUES (?, ?, ?, ?, ?, ?, ?)
  `).run('ses_roundtrip', 'proj1', '/work/proj1', 'Roundtrip Session', '2.0.32', 1000, 1000);
  db.prepare(`
    INSERT INTO session_message (id, session_id, type, seq, time_created, time_updated, data)
    VALUES (?, ?, ?, ?, ?, ?, ?)
  `).run('m1', 'ses_roundtrip', 'user', 1, 1000, 1000, JSON.stringify({ text: 'prompt' }));
  db.prepare(`
    INSERT INTO session_message (id, session_id, type, seq, time_created, time_updated, data)
    VALUES (?, ?, ?, ?, ?, ?, ?)
  `).run('m2', 'ses_roundtrip', 'assistant', 2, 1005, 1005, JSON.stringify({
    content: [
      { type: 'reasoning', text: 'thinking' },
      { type: 'tool', id: 'c1', name: 'read', state: { status: 'completed', input: { path: 'file.txt' }, content: 'hello' } },
      { type: 'text', text: 'response' }
    ],
    tokens: { input: 10, output: 5 }
  }));
  db.close();

  const provider = createOpencodeProvider({ rootDir: dir, homeDir: home, openDatabase: openDb });
  const units = discover(provider);
  const { values: canonicalRecords } = drain(provider.parse(units[0], null));

  // Assemble directly from canonical generator stream
  const directDetail = assembleSessionDetail(canonicalRecords);

  // Round-trip through SQLite persist layer
  const memDb = createMemoryIndex();
  persist(memDb, units[0], provider.parse(units[0], null));

  const roundTripRows = memDb.prepare(`
    SELECT * FROM messages WHERE session_id = ? ORDER BY timestamp, rowid
  `).all(units[0].sessionId);
  const sessionRow = memDb.prepare('SELECT * FROM sessions WHERE id = ?').get(units[0].sessionId);
  const toolCalls = memDb.prepare('SELECT * FROM tool_calls WHERE session_id = ?').all(units[0].sessionId);
  const toolResults = memDb.prepare('SELECT * FROM tool_results WHERE session_id = ?').all(units[0].sessionId);

  const reassembledDetail = assembleSessionDetail({
    session: sessionRow,
    messages: roundTripRows,
    toolCalls,
    toolResults,
  });

  assert.deepEqual(directDetail, reassembledDetail);
  assert.equal(directDetail.session.title, reassembledDetail.session.title);
  assert.equal(directDetail.session.message_count, reassembledDetail.session.message_count);
});

function indexFixture(provider) {
  const db = new DatabaseSync(':memory:');
  db.exec(SCHEMA);
  const registry = createProviderRegistry([provider]);
  const run = (options = {}) => {
    const plan = createProviderIndexPlan(db, registry, options);
    const result = indexProviderPlan({
      db,
      plan,
      runTransaction: (_label, work) => runWriteTransaction(db, work),
      onError: (error) => {
        throw error;
      },
    });
    return { plan, result };
  };
  return { db, run, close: () => db.close() };
}

test('incremental index update matches fresh build from scratch (delta = fresh)', () => {
  const home = makeTempDir('opencode-delta-fresh-');
  const dir = join(home, 'data');
  mkdirSync(dir, { recursive: true });
  const dbPath = join(dir, 'opencode.db');
  initTestStore(dbPath);

  const db = new DatabaseSync(dbPath);
  db.prepare(`
    INSERT INTO session_v2 (id, project_id, directory, title, version, time_created, time_updated)
    VALUES (?, ?, ?, ?, ?, ?, ?)
  `).run('ses_1', 'proj1', '/work/proj1', 'Session 1', '2.0.32', 1000, 1000);
  db.prepare(`
    INSERT INTO session_message (id, session_id, type, seq, time_created, time_updated, data)
    VALUES (?, ?, ?, ?, ?, ?, ?)
  `).run('m1_1', 'ses_1', 'user', 1, 1000, 1000, JSON.stringify({ text: 'msg 1' }));
  db.close();

  const provider = createOpencodeProvider({ rootDir: dir, homeDir: home, openDatabase: openDb });

  // Initial index build
  const idx = indexFixture(provider);
  idx.run({ force: false });

  // Now update session 1 in source database
  const dbUpdate = new DatabaseSync(dbPath);
  dbUpdate.prepare(`
    UPDATE session_v2 SET time_updated = 2000, title = 'Session 1 Updated' WHERE id = 'ses_1'
  `).run();
  dbUpdate.prepare(`
    INSERT INTO session_message (id, session_id, type, seq, time_created, time_updated, data)
    VALUES (?, ?, ?, ?, ?, ?, ?)
  `).run('m1_2', 'ses_1', 'user', 2, 2000, 2000, JSON.stringify({ text: 'msg 2' }));
  dbUpdate.close();

  // Incremental build
  const incRun = idx.run({ force: false });
  assert.equal(incRun.plan.items.length, 1, 'only changed session scheduled');

  // Fresh build in separate index
  const freshIdx = indexFixture(provider);
  freshIdx.run({ force: true });

  const incSessions = idx.db.prepare('SELECT id, title, message_count FROM sessions ORDER BY id').all();
  const freshSessions = freshIdx.db.prepare('SELECT id, title, message_count FROM sessions ORDER BY id').all();
  assert.deepEqual(incSessions, freshSessions);

  const incMessages = idx.db.prepare('SELECT uuid, text FROM messages ORDER BY uuid').all();
  const freshMessages = freshIdx.db.prepare('SELECT uuid, text FROM messages ORDER BY uuid').all();
  assert.deepEqual(incMessages, freshMessages);

  idx.close();
  freshIdx.close();
});

test('fault tolerance: corrupt or busy database does not tombstone prior sessions', () => {
  const home = makeTempDir('opencode-faults-');
  const dir = join(home, 'data');
  mkdirSync(dir, { recursive: true });
  const dbPath = join(dir, 'opencode.db');

  // 1. Corrupt / non-sqlite file
  writeFileSync(dbPath, 'garbage non-sqlite content');

  const provider = createOpencodeProvider({ rootDir: dir, homeDir: home, openDatabase: openDb });
  const issues = [];
  const indexed = [{
    sessionId: opencodeSessionId('prior_session', dbPath),
    jsonlPath: opencodeUnitKey(dbPath, 'prior_session'),
  }];

  const units = discover(provider, { indexed, issues });
  assert.deepEqual(units, [], 'corrupt file does not emit tombstones');
  assert.equal(issues.length, 1);
  assert.match(issues[0].error, /tables|busy|unreadable/i);

  // 2. Real SQLITE_BUSY thrown by opener
  const busyProvider = createOpencodeProvider({
    rootDir: dir,
    homeDir: home,
    openDatabase: () => {
      throw Object.assign(new Error('database is locked'), { code: 'SQLITE_BUSY' });
    },
  });
  const busyIssues = [];
  const busyUnits = discover(busyProvider, { indexed, issues: busyIssues });
  assert.deepEqual(busyUnits, [], 'busy database does not emit tombstones');
  assert.equal(busyIssues.length, 1);
  assert.match(busyIssues[0].error, /busy|locked|unreadable/i);
});

test('watchTargets() includes db, wal and channel db fileNames without shm', () => {
  const home = makeTempDir('opencode-watch-');
  const dir = join(home, 'data');
  mkdirSync(dir, { recursive: true });
  writeFileSync(join(dir, 'opencode.db'), '');
  writeFileSync(join(dir, 'opencode-dev.db'), '');
  writeFileSync(join(dir, 'opencode.db-shm'), ''); // Must not be watched

  const provider = createOpencodeProvider({ rootDir: dir, homeDir: home, openDatabase: openDb });
  const targets = provider.watchTargets(dir);

  const fileTargets = targets.filter(t => t.kind === 'file');
  const treeTargets = targets.filter(t => t.kind === 'tree');

  assert.equal(fileTargets.some(t => t.path.endsWith('opencode.db-shm')), false);
  assert.ok(treeTargets.length > 0);
  assert.ok(treeTargets[0].fileNames.includes('opencode.db'));
  assert.ok(treeTargets[0].matches('opencode-dev.db'));
  assert.ok(treeTargets[0].matches('opencode-dev.db-wal'));
  assert.equal(treeTargets[0].matches('opencode.db-shm'), false);
});

test('parses real captured OpenCode V1 and V2 store rows from fixtures', () => {
  const fixturePath = new URL('./fixtures/opencode/real-store-rows.json', import.meta.url);
  const fixture = JSON.parse(readFileSync(fixturePath, 'utf8'));

  const home = makeTempDir('opencode-fixtures-real-');
  const dir = join(home, 'data');
  mkdirSync(dir, { recursive: true });
  const dbPath = join(dir, 'opencode.db');
  initTestStore(dbPath);

  const db = new DatabaseSync(dbPath);
  // Insert V1 rows
  for (const s of fixture.v1.sessions) {
    db.prepare(`
      INSERT INTO session (id, project_id, directory, title, version, time_created, time_updated)
      VALUES (?, ?, ?, ?, ?, ?, ?)
    `).run(s.id, s.project_id || 'proj', s.directory, s.title, s.version, s.time_created, s.time_updated);
  }
  for (const m of fixture.v1.messages) {
    db.prepare(`
      INSERT INTO message (id, session_id, time_created, time_updated, data)
      VALUES (?, ?, ?, ?, ?)
    `).run(m.id, m.session_id, m.time_created, m.time_updated, m.data);
  }
  for (const p of fixture.v1.parts) {
    db.prepare(`
      INSERT INTO part (id, message_id, session_id, time_created, time_updated, data)
      VALUES (?, ?, ?, ?, ?, ?)
    `).run(p.id, p.message_id, p.session_id, p.time_created, p.time_updated, p.data);
  }

  // Insert V2 rows
  for (const s of fixture.v2.sessions) {
    db.prepare(`
      INSERT INTO session_v2 (id, project_id, parent_id, directory, title, version, time_created, time_updated, agent)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(s.id, s.project_id || 'proj', s.parent_id || null, s.directory, s.title, s.version, s.time_created, s.time_updated, s.agent || null);
  }
  for (const m of fixture.v2.messages) {
    db.prepare(`
      INSERT INTO session_message (id, session_id, type, seq, time_created, time_updated, data)
      VALUES (?, ?, ?, ?, ?, ?, ?)
    `).run(m.id, m.session_id, m.type, m.seq, m.time_created, m.time_updated, m.data);
  }
  db.close();

  const provider = createOpencodeProvider({ rootDir: dir, homeDir: home, openDatabase: openDb });
  const units = discover(provider);

  assert.equal(units.length, fixture.v1.sessions.length + fixture.v2.sessions.length);

  for (const unit of units) {
    const { values, cursor } = drain(provider.parse(unit, null));
    assert.ok(cursor, 'parse returned valid cursor');
    assert.ok(values.length > 0, 'emitted canonical records');

    const sessionRecord = values.find(r => r.kind === 'session');
    assert.ok(sessionRecord, 'session record emitted');
    assert.equal(sessionRecord.source, 'opencode');

    // Canonical assembly roundtrip
    const detail = assembleSessionDetail(values);
    assert.ok(detail.session);
    assert.equal(detail.session.id, unit.sessionId);

    // ADR-0007 full assembled object deepEqual after SQLite round-trip
    const memDb = new DatabaseSync(':memory:');
    memDb.exec(SCHEMA);
    persist(memDb, unit, provider.parse(unit, null));
    const roundTripRows = memDb.prepare('SELECT * FROM messages WHERE session_id = ? ORDER BY timestamp, rowid').all(unit.sessionId);
    const sessionRow = memDb.prepare('SELECT * FROM sessions WHERE id = ?').get(unit.sessionId);
    const toolCalls = memDb.prepare('SELECT * FROM tool_calls WHERE session_id = ?').all(unit.sessionId);
    const toolResults = memDb.prepare('SELECT * FROM tool_results WHERE session_id = ?').all(unit.sessionId);
    const summaries = memDb.prepare('SELECT * FROM summaries WHERE session_id = ?').all(unit.sessionId);
    const reassembled = assembleSessionDetail({
      session: sessionRow,
      messages: roundTripRows,
      toolCalls,
      toolResults,
      summaries,
    });
    assert.deepEqual(detail, reassembled, `assembled session detail deepEqual for ${unit.sessionId}`);
    memDb.close();
  }
});

test('census and parse fallback to V1 when V2 session disappears (V2消失V1仍存要回退)', () => {
  const home = makeTempDir('opencode-fallback-v1-');
  const dir = join(home, 'data');
  mkdirSync(dir, { recursive: true });
  const dbPath = join(dir, 'opencode.db');
  initTestStore(dbPath);

  const db = new DatabaseSync(dbPath);
  // Session exists in both V1 and V2 initially
  db.prepare(`
    INSERT INTO session (id, project_id, directory, title, version, time_created, time_updated)
    VALUES (?, ?, ?, ?, ?, ?, ?)
  `).run('ses_migrate_back', 'proj1', '/work/proj1', 'V1 Original', '1.19.3', 1000, 1000);
  db.prepare(`
    INSERT INTO message (id, session_id, time_created, time_updated, data)
    VALUES (?, ?, ?, ?, ?)
  `).run('m_v1_1', 'ses_migrate_back', 1000, 1000, JSON.stringify({ role: 'user', text: 'v1 original text' }));

  db.prepare(`
    INSERT INTO session_v2 (id, project_id, directory, title, version, time_created, time_updated)
    VALUES (?, ?, ?, ?, ?, ?, ?)
  `).run('ses_migrate_back', 'proj1', '/work/proj1', 'V2 Modern', '2.0.32', 2000, 2000);
  db.prepare(`
    INSERT INTO session_message (id, session_id, type, seq, time_created, time_updated, data)
    VALUES (?, ?, ?, ?, ?, ?, ?)
  `).run('m_v2_1', 'ses_migrate_back', 'user', 1, 2000, 2000, JSON.stringify({ text: 'v2 modern text' }));
  db.close();

  const provider = createOpencodeProvider({ rootDir: dir, homeDir: home, openDatabase: openDb });
  const idx = indexFixture(provider);

  // Initial index: V2 is selected
  const run1 = idx.run({ force: false });
  assert.equal(run1.plan.items.length, 1);
  const sRow1 = idx.db.prepare('SELECT id, title, version FROM sessions WHERE source = ?').get('opencode');
  assert.equal(sRow1.title, 'V2 Modern');
  assert.equal(sRow1.version, '2.0.32');

  // Now V2 session disappears (deleted from session_v2 and session_message), but V1 survives
  const dbDel = new DatabaseSync(dbPath);
  dbDel.prepare('DELETE FROM session_message WHERE session_id = ?').run('ses_migrate_back');
  dbDel.prepare('DELETE FROM session_v2 WHERE id = ?').run('ses_migrate_back');
  dbDel.close();

  // Re-indexing: provider detects V2 is gone and falls back to V1
  const run2 = idx.run({ force: false });
  assert.equal(run2.plan.items.length, 1, 'session is rescheduled after V2 removal');
  assert.equal(run2.plan.items[0].unit.meta.format, 'v1');

  const sRow2 = idx.db.prepare('SELECT id, title, version FROM sessions WHERE source = ?').get('opencode');
  assert.equal(sRow2.title, 'V1 Original', 'reverts to surviving V1 metadata');
  assert.equal(sRow2.version, '1.19.3');

  const mRows2 = idx.db.prepare('SELECT uuid, text FROM messages WHERE session_id = ?').all(sRow2.id);
  assert.equal(mRows2.length, 1);
  assert.equal(mRows2[0].text, 'v1 original text');

  idx.close();
});

test('provider-settings runtime preserves explicit vs automatic root across OPENCODE_DB conflict, deletion, restart, and invalid root', async () => {
  const home = makeTempDir('opencode-settings-conflict-');
  const customExplicitDir = join(home, 'custom-opencode');
  mkdirSync(customExplicitDir, { recursive: true });
  const customDbPath = join(customExplicitDir, 'opencode.db');
  initTestStore(customDbPath);
  const dbCustom = new DatabaseSync(customDbPath);
  dbCustom.prepare(`
    INSERT INTO session_v2 (id, project_id, directory, title, version, time_created, time_updated, slug)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
  `).run('ses_custom', 'proj1', '/work/custom', 'Custom Session', '2.0.32', 1000, 1000, 'slug_custom');
  dbCustom.close();

  const envDir = join(home, 'env-opencode');
  mkdirSync(envDir, { recursive: true });
  const envDbPath = join(envDir, 'opencode.db');
  initTestStore(envDbPath);
  const dbEnv = new DatabaseSync(envDbPath);
  dbEnv.prepare(`
    INSERT INTO session_v2 (id, project_id, directory, title, version, time_created, time_updated, slug)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
  `).run('ses_env', 'proj2', '/work/env', 'Env Session', '2.0.32', 2000, 2000, 'slug_env');
  dbEnv.close();

  const prevEnv = process.env.OPENCODE_DB;
  try {
    process.env.OPENCODE_DB = envDbPath;

    // 1. Explicit setting in providerRoots takes precedence over OPENCODE_DB
    const { createConfiguredBuiltinProviderRuntime } = await import('../packages/core/src/provider-settings.ts');
    const settingsExplicit = {
      providerRoots: {
        opencode: customExplicitDir,
      },
    };
    const runtime1 = createConfiguredBuiltinProviderRuntime(settingsExplicit, {
      homeDir: home,
      openOpencodeDatabase: openDb,
    });
    const opencode1 = runtime1.registry.get('opencode');
    assert.equal(opencode1.descriptor.defaultRoot, customExplicitDir);
    // Discovers store in customExplicitDir, NOT envDir
    const units1 = discover(opencode1);
    assert.equal(units1.length, 1);
    assert.ok(units1[0].key.startsWith(customExplicitDir));
    assert.equal(units1[0].sessionId, opencodeSessionId('ses_custom', customDbPath));

    // 2. Deleting settings restores automatic environment (OPENCODE_DB)
    const settingsRestored = {
      providerRoots: {},
    };
    const runtime2 = createConfiguredBuiltinProviderRuntime(settingsRestored, {
      homeDir: home,
      openOpencodeDatabase: openDb,
    });
    const opencode2 = runtime2.registry.get('opencode');
    assert.equal(opencode2.descriptor.defaultRoot, join(home, '.local', 'share', 'opencode'));
    // Discovers store in envDir via OPENCODE_DB
    const units2 = discover(opencode2);
    assert.equal(units2.length, 1);
    assert.ok(units2[0].key.startsWith(envDir));
    assert.equal(units2[0].sessionId, opencodeSessionId('ses_env', envDbPath));

    // 3. Restart with empty settings object also preserves automatic environment
    const runtimeRestart = createConfiguredBuiltinProviderRuntime({}, {
      homeDir: home,
      openOpencodeDatabase: openDb,
    });
    const opencodeRestart = runtimeRestart.registry.get('opencode');
    assert.equal(opencodeRestart.descriptor.defaultRoot, join(home, '.local', 'share', 'opencode'));

    // 4. Invalid root (relative path like './relative') disables provider rather than selecting default
    const settingsInvalid = {
      providerRoots: {
        opencode: './invalid-relative-path',
      },
    };
    const runtimeInvalid = createConfiguredBuiltinProviderRuntime(settingsInvalid, {
      homeDir: home,
      openOpencodeDatabase: openDb,
    });
    const opencodeInvalid = runtimeInvalid.registry.get('opencode');
    assert.equal(opencodeInvalid.descriptor.requiresExplicitRoot, true);
    assert.deepEqual(opencodeInvalid.watchTargets(opencodeInvalid.descriptor.defaultRoot), []);
    assert.deepEqual(discover(opencodeInvalid), []);
  } finally {
    if (prevEnv !== undefined) process.env.OPENCODE_DB = prevEnv; else delete process.env.OPENCODE_DB;
  }
});

test('P1-1: tombstone verification throws on missing/busy source DB and rolls back transaction preserving last-good state', () => {
  const home = makeTempDir('opencode-tomb-p11-');
  const dir = join(home, 'data');
  mkdirSync(dir, { recursive: true });
  const dbPath = join(dir, 'opencode.db');
  initTestStore(dbPath);

  const dbSrc = new DatabaseSync(dbPath);
  dbSrc.prepare(`
    INSERT INTO session_v2 (id, project_id, directory, title, version, time_created, time_updated, slug)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
  `).run('ses_tomb_1', 'p1', '/work/p1', 'Active Session', '2.0.32', 1000, 1000, 'slug_1');
  dbSrc.prepare(`
    INSERT INTO session_message (id, session_id, type, seq, time_created, time_updated, data)
    VALUES (?, ?, ?, ?, ?, ?, ?)
  `).run('m1', 'ses_tomb_1', 'user', 1, 1000, 1000, JSON.stringify({ text: 'important data' }));
  dbSrc.close();

  let openerShouldFail = false;
  const customOpener = (path) => {
    if (openerShouldFail) {
      throw Object.assign(new Error('database is locked'), { code: 'SQLITE_BUSY' });
    }
    return openDb(path);
  };

  const provider = createOpencodeProvider({ rootDir: dir, homeDir: home, openDatabase: customOpener });
  const idx = indexFixture(provider);

  // 1. Initial successful index
  const run1 = idx.run();
  assert.equal(run1.result.committed.length, 1);
  const sRowBefore = idx.db.prepare('SELECT id, title FROM sessions WHERE id = ?').get(run1.result.committed[0].unit.sessionId);
  assert.ok(sRowBefore);
  const msgBefore = idx.db.prepare('SELECT * FROM messages WHERE session_id = ?').all(sRowBefore.id);
  assert.equal(msgBefore.length, 1);
  const cursorBefore = idx.db.prepare('SELECT cursor FROM index_state WHERE jsonl_path = ?').get(run1.result.committed[0].unit.key);
  assert.ok(cursorBefore);

  // 2. Delete session from source DB while keeping DB accessible -> discover returns tombstone unit
  const dbDel = new DatabaseSync(dbPath);
  dbDel.prepare('DELETE FROM session_message WHERE session_id = ?').run('ses_tomb_1');
  dbDel.prepare('DELETE FROM session_v2 WHERE id = ?').run('ses_tomb_1');
  dbDel.close();

  // Run discovery to capture the tombstone plan
  const plan = createProviderIndexPlan(idx.db, createProviderRegistry([provider]));
  assert.equal(plan.items.length, 1);
  const tombUnit = plan.items[0].unit;
  assert.equal(tombUnit.meta.rawSessionId, '');

  // 3a. Case A: source DB renamed offline before parse executes
  renameSync(dbPath, join(dir, 'opencode.offline.db'));

  let caughtA = null;
  try {
    indexProviderPlan({
      db: idx.db,
      plan,
      runTransaction: (_label, work) => runWriteTransaction(idx.db, work),
      onError: (err) => {
        caughtA = err;
        return 'stop';
      },
    });
  } finally {
    renameSync(join(dir, 'opencode.offline.db'), dbPath);
  }

  assert.ok(caughtA);
  assert.match(caughtA.message, /busy or unreadable/i);
  // Assert rollback preserved session, messages, and old cursor
  const sRowAfterA = idx.db.prepare('SELECT id, title FROM sessions WHERE id = ?').get(sRowBefore.id);
  assert.ok(sRowAfterA, 'session preserved after failed tombstone parse (missing DB)');
  const msgAfterA = idx.db.prepare('SELECT * FROM messages WHERE session_id = ?').all(sRowBefore.id);
  assert.equal(msgAfterA.length, 1, 'messages preserved after failed tombstone parse');
  const cursorAfterA = idx.db.prepare('SELECT cursor FROM index_state WHERE jsonl_path = ?').get(tombUnit.key);
  assert.equal(cursorAfterA.cursor, cursorBefore.cursor, 'old cursor preserved');

  // 3b. Case B: opener throws SQLITE_BUSY
  openerShouldFail = true;
  let caughtB = null;
  indexProviderPlan({
    db: idx.db,
    plan,
    runTransaction: (_label, work) => runWriteTransaction(idx.db, work),
    onError: (err) => {
      caughtB = err;
      return 'stop';
    },
  });

  assert.ok(caughtB);
  assert.match(caughtB.message, /busy or unreadable/i);
  const sRowAfterB = idx.db.prepare('SELECT id, title FROM sessions WHERE id = ?').get(sRowBefore.id);
  assert.ok(sRowAfterB, 'session preserved after failed tombstone parse (BUSY DB)');
  const cursorAfterB = idx.db.prepare('SELECT cursor FROM index_state WHERE jsonl_path = ?').get(tombUnit.key);
  assert.equal(cursorAfterB.cursor, cursorBefore.cursor, 'old cursor preserved');

  // 4. Case C: Now DB is accessible and confirmed deleted -> tombstone succeeds cleanly
  openerShouldFail = false;
  const resultOk = indexProviderPlan({
    db: idx.db,
    plan,
    runTransaction: (_label, work) => runWriteTransaction(idx.db, work),
    onError: (err) => { throw err; },
  });
  assert.equal(resultOk.committed.length, 1);
  const sRowFinal = idx.db.prepare('SELECT id FROM sessions WHERE id = ?').get(sRowBefore.id);
  assert.equal(sRowFinal, undefined, 'session cleanly retracted only after verified deletion');

  idx.close();
});

test('P1-2: multi-channel census: unavailable store blocks destructive force build, preserves old snapshot', () => {
  const home = makeTempDir('opencode-p12-');
  const dir = join(home, 'data');
  mkdirSync(dir, { recursive: true });
  const standardDb = join(dir, 'opencode.db');
  const devDb = join(dir, 'opencode-dev.db');
  initTestStore(standardDb);
  initTestStore(devDb);

  const dbStd = new DatabaseSync(standardDb);
  dbStd.prepare(`INSERT INTO session_v2 (id, project_id, directory, title, version, time_created, time_updated, slug) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`).run('ses_std', 'p1', '/work/p1', 'Standard Session', '2.0.32', 1000, 1000, 'slug_std');
  dbStd.close();

  const dbDev = new DatabaseSync(devDb);
  dbDev.prepare(`INSERT INTO session_v2 (id, project_id, directory, title, version, time_created, time_updated, slug) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`).run('ses_dev', 'p1', '/work/p1', 'Dev Session', '2.0.32', 2000, 2000, 'slug_dev');
  dbDev.close();

  const provider = createOpencodeProvider({ rootDir: dir, homeDir: home, openDatabase: openDb });
  const idx = indexFixture(provider);

  // 1. Initial build: both stores discovered and indexed
  const run1 = idx.run({ force: true });
  assert.equal(run1.result.committed.length, 2);
  const allSessions1 = idx.db.prepare('SELECT id FROM sessions').all();
  assert.equal(allSessions1.length, 2);

  // 2. devDb goes offline (renamed)
  renameSync(devDb, join(dir, 'opencode-dev.offline'));

  // 3. Discovery reports incomplete inventory for unavailable store
  const issues = [];
  provider.discover({
    lastCursor: () => null,
    indexedSessions: () => idx.db.prepare('SELECT id as sessionId, jsonl_path as jsonlPath, source FROM sessions').all(),
    reportIncompleteInventory: (issue) => issues.push(issue),
  });

  assert.equal(issues.length, 1);
  assert.equal(issues[0].path, normalize(devDb));
  assert.match(issues[0].error, /unavailable/i);

  // 4. Force build plan sets incompleteProviders = ['opencode']
  const registry = createProviderRegistry([provider]);
  const plan = createProviderIndexPlan(idx.db, registry, { force: true });
  assert.ok(plan.incompleteProviders.has('opencode'), 'force plan marks opencode incomplete when store is unavailable');

  // 5. Restore devDb
  renameSync(join(dir, 'opencode-dev.offline'), devDb);

  const issuesRecovered = [];
  const unitsRecovered = provider.discover({
    lastCursor: () => null,
    indexedSessions: () => idx.db.prepare('SELECT id as sessionId, jsonl_path as jsonlPath, source FROM sessions').all(),
    reportIncompleteInventory: (issue) => issuesRecovered.push(issue),
  });
  assert.equal(issuesRecovered.length, 0);
  assert.equal(unitsRecovered.length, 2);

  idx.close();
});

test('P2-3: strict raw lookup verification rejects forged session prefixes, seqs, and block indices', () => {
  const home = makeTempDir('opencode-raw-strict-');
  const dir = join(home, 'data');
  mkdirSync(dir, { recursive: true });
  const dbPath = join(dir, 'opencode.db');
  initTestStore(dbPath);

  const db = new DatabaseSync(dbPath);
  db.prepare(`INSERT INTO session_v2 (id, project_id, directory, title, version, time_created, time_updated, slug) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`).run('ses_legit', 'p1', '/work/p1', 'Title', '2.0.32', 1000, 1000, 'slug_1');
  db.prepare(`INSERT INTO session_v2 (id, project_id, directory, title, version, time_created, time_updated, slug) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`).run('ses_other', 'p1', '/work/p1', 'Other', '2.0.32', 1000, 1000, 'slug_2');
  db.prepare(`INSERT INTO session_message (id, session_id, type, seq, time_created, time_updated, data) VALUES (?, ?, ?, ?, ?, ?, ?)`).run('msg_1', 'ses_legit', 'assistant', 1, 1000, 1000, JSON.stringify({
    content: [{ type: 'text', text: 'AAA' }],
  }));
  db.close();

  const provider = createOpencodeProvider({ rootDir: dir, homeDir: home, openDatabase: openDb });
  const sessionId = opencodeSessionId('ses_legit', dbPath);
  const otherSessionId = opencodeSessionId('ses_other', dbPath);
  const jsonlPath = opencodeUnitKey(dbPath, 'ses_legit');

  const validUuid = `${sessionId}:m:000001:msg_1:c:0000:text`;

  // 1. Valid lookup succeeds
  const ok = provider.raw({ source: 'opencode', messageUuid: validUuid, session: { id: sessionId, jsonl_path: jsonlPath } });
  assert.ok(ok);
  assert.equal(ok.messageText, 'AAA');

  // 2. Forged UUID prefix (different session in same store)
  const fakePrefixUuid = `${otherSessionId}:m:000001:msg_1:c:0000:text`;
  assert.equal(provider.raw({ source: 'opencode', messageUuid: fakePrefixUuid, session: { id: sessionId, jsonl_path: jsonlPath } }), null);

  // 3. Forged seq 999999
  const fakeSeqUuid = `${sessionId}:m:999999:msg_1:c:0000:text`;
  assert.equal(provider.raw({ source: 'opencode', messageUuid: fakeSeqUuid, session: { id: sessionId, jsonl_path: jsonlPath } }), null);

  // 4. Forged block 9999
  const fakeBlockUuid = `${sessionId}:m:000001:msg_1:c:9999:text`;
  assert.equal(provider.raw({ source: 'opencode', messageUuid: fakeBlockUuid, session: { id: sessionId, jsonl_path: jsonlPath } }), null);

  // 5. session.id and jsonl_path contradiction
  assert.equal(provider.raw({ source: 'opencode', messageUuid: validUuid, session: { id: otherSessionId, jsonl_path: jsonlPath } }), null);

  // 6. Block kind mismatch (asking for :tool when block 0 is text)
  const wrongKindUuid = `${sessionId}:m:000001:msg_1:c:0000:tool`;
  assert.equal(provider.raw({ source: 'opencode', messageUuid: wrongKindUuid, session: { id: sessionId, jsonl_path: jsonlPath } }), null);
});

test('P2-4: raw lookup queries run in a single snapshot read transaction (WAL concurrent write isolation)', () => {
  const home = makeTempDir('opencode-raw-race-');
  const dir = join(home, 'data');
  mkdirSync(dir, { recursive: true });
  const dbPath = join(dir, 'opencode.db');
  initTestStore(dbPath);

  const db = new DatabaseSync(dbPath);
  db.exec('PRAGMA journal_mode = WAL;');
  db.prepare(`INSERT INTO session_v2 (id, project_id, directory, title, version, time_created, time_updated, slug) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`).run('ses_race', 'p1', '/work/p1', 'Race', '2.0.32', 1000, 1000, 'slug_race');
  db.prepare(`INSERT INTO session_message (id, session_id, type, seq, time_created, time_updated, data) VALUES (?, ?, ?, ?, ?, ?, ?)`).run('m1', 'ses_race', 'user', 1, 1000, 1000, JSON.stringify({ text: 'OLD' }));
  db.close();

  const provider = createOpencodeProvider({ rootDir: dir, homeDir: home, openDatabase: openDb });
  const units = provider.discover({ lastCursor: () => null });
  const records = drain(provider.parse(units[0], null));
  const validCursor = records.cursor; // cursor for 'OLD'

  const sessionId = opencodeSessionId('ses_race', dbPath);
  const jsonlPath = opencodeUnitKey(dbPath, 'ses_race');
  const uuid = `${sessionId}:m:000001:m1:msg`;

  // Connection B commits an update OLD -> NEW
  const dbB = new DatabaseSync(dbPath);
  dbB.prepare('UPDATE session_message SET data = ? WHERE id = ?').run(JSON.stringify({ text: 'NEW' }), 'm1');
  dbB.close();

  // Looking up with OLD cursor should fail because the snapshot fingerprint changed (returns null, never NEW with old cursor)
  const resultOldCursor = provider.raw({
    source: 'opencode',
    messageUuid: uuid,
    cursor: validCursor,
    session: { id: sessionId, jsonl_path: jsonlPath },
  });
  assert.equal(resultOldCursor, null, 'old cursor rejects updated database content');
});

test('P2-5: token accounting: textless step-finish tokens, empty step anchor, and V2 failed compaction tokens', () => {
  const home = makeTempDir('opencode-tokens-p25-');
  const dir = join(home, 'data');
  mkdirSync(dir, { recursive: true });
  const dbPath = join(dir, 'opencode.db');
  initTestStore(dbPath);

  const db = new DatabaseSync(dbPath);
  // V1 assistant with textless step-finish with tokens 10/2 and message fallback 10/2
  db.prepare(`INSERT INTO session (id, project_id, directory, title, version, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?, ?)`).run('ses_v1_textless', 'p1', '/work/p1', 'Textless Step', '1.19.3', 1000, 1000);
  db.prepare(`INSERT INTO message (id, session_id, time_created, time_updated, data) VALUES (?, ?, ?, ?, ?)`).run('msg_v1', 'ses_v1_textless', 1000, 1000, JSON.stringify({
    role: 'assistant',
    tokens: { input: 10, output: 2 },
  }));
  db.prepare(`INSERT INTO part VALUES (?, ?, ?, ?, ?, ?)`).run('p_01_step', 'msg_v1', 'ses_v1_textless', 1000, 1000, JSON.stringify({
    type: 'step-finish',
    tokens: { input: 10, output: 2 },
  }));

  // V2 failed compaction with tokens
  db.prepare(`INSERT INTO session_v2 (id, project_id, directory, title, version, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?, ?)`).run('ses_v2_comp', 'p1', '/work/p1', 'Failed Comp', '2.0.32', 2000, 2000);
  db.prepare(`INSERT INTO session_message (id, session_id, type, seq, time_created, time_updated, data) VALUES (?, ?, ?, ?, ?, ?, ?)`).run('m_comp_fail', 'ses_v2_comp', 'compaction', 1, 2000, 2000, JSON.stringify({
    status: 'failed',
    summary: null,
    tokens: { input: 50, output: 15 },
  }));
  db.close();

  const provider = createOpencodeProvider({ rootDir: dir, homeDir: home, openDatabase: openDb });
  const units = provider.discover({ lastCursor: () => null });

  const uV1 = units.find(u => u.sessionId.includes('ses_v1_textless'));
  const v1Records = [...provider.parse(uV1, null)].filter(r => r.kind === 'message');
  assert.equal(v1Records.length, 1);
  assert.equal(v1Records[0].input_tokens, 10);
  assert.equal(v1Records[0].output_tokens, 2);

  const uV2 = units.find(u => u.sessionId.includes('ses_v2_comp'));
  const v2Records = [...provider.parse(uV2, null)].filter(r => r.kind === 'message');
  assert.equal(v2Records.length, 1, 'failed compaction with tokens emits accounting record');
  assert.equal(v2Records[0].input_tokens, 50);
  assert.equal(v2Records[0].output_tokens, 15);
  assert.equal(v2Records[0].visibility, 'hidden');
});

test('P2-6: authentic Model.Ref object resolution for V2 assistant, switch card, and V1 modelID', () => {
  const home = makeTempDir('opencode-models-p26-');
  const dir = join(home, 'data');
  mkdirSync(dir, { recursive: true });
  const dbPath = join(dir, 'opencode.db');
  initTestStore(dbPath);

  const db = new DatabaseSync(dbPath);
  // V2 session with session.model JSON and per-message model object
  db.prepare(`INSERT INTO session_v2 (id, project_id, directory, title, version, time_created, time_updated, model) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`).run(
    'ses_v2_model', 'p1', '/work/p1', 'Model Test', '2.0.32', 1000, 1000, JSON.stringify({ id: 'session-default-model', providerID: 'test' }),
  );
  db.prepare(`INSERT INTO session_message (id, session_id, type, seq, time_created, time_updated, data) VALUES (?, ?, ?, ?, ?, ?, ?)`).run(
    'm1_asst', 'ses_v2_model', 'assistant', 1, 1000, 1000, JSON.stringify({
      model: { id: 'actual-per-message-model', providerID: 'custom-p' },
      content: [{ type: 'text', text: 'hello' }],
    }),
  );
  db.prepare(`INSERT INTO session_message (id, session_id, type, seq, time_created, time_updated, data) VALUES (?, ?, ?, ?, ?, ?, ?)`).run(
    'm2_switch', 'ses_v2_model', 'model-switched', 2, 1001, 1001, JSON.stringify({
      model: { id: 'target-model', providerID: 'custom-p' },
      previous: { id: 'source-model', providerID: 'custom-p' },
    }),
  );

  // V1 session with modelID
  db.prepare(`INSERT INTO session (id, project_id, directory, title, version, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?, ?)`).run('ses_v1_model', 'p1', '/work/p1', 'V1 Model', '1.19.3', 2000, 2000);
  db.prepare(`INSERT INTO message (id, session_id, time_created, time_updated, data) VALUES (?, ?, ?, ?, ?)`).run('m1_v1', 'ses_v1_model', 2000, 2000, JSON.stringify({
    role: 'assistant',
    modelID: 'v1-native-model',
  }));
  db.prepare(`INSERT INTO part VALUES (?, ?, ?, ?, ?, ?)`).run('p1', 'm1_v1', 'ses_v1_model', 2000, 2000, JSON.stringify({ type: 'text', text: 'v1 resp' }));
  db.close();

  const provider = createOpencodeProvider({ rootDir: dir, homeDir: home, openDatabase: openDb });
  const units = provider.discover({ lastCursor: () => null });

  const uV2 = units.find(u => u.sessionId.includes('ses_v2_model'));
  const v2Msgs = [...provider.parse(uV2, null)].filter(r => r.kind === 'message');
  assert.equal(v2Msgs[0].model, 'actual-per-message-model', 'V2 assistant parses model.id');
  assert.equal(v2Msgs[1].text, 'model source-model → target-model', 'V2 switch card resolves objects');
  assert.equal(v2Msgs[1].model, 'target-model');

  const uV1 = units.find(u => u.sessionId.includes('ses_v1_model'));
  const v1Msgs = [...provider.parse(uV1, null)].filter(r => r.kind === 'message');
  assert.equal(v1Msgs[0].model, 'v1-native-model', 'V1 assistant parses modelID');
});

test('P2-7: V1 user multi-part revert partitions into visible and inactive parts', () => {
  const home = makeTempDir('opencode-revert-user-');
  const dir = join(home, 'data');
  mkdirSync(dir, { recursive: true });
  const dbPath = join(dir, 'opencode.db');
  initTestStore(dbPath);

  const db = new DatabaseSync(dbPath);
  db.prepare(`INSERT INTO session (id, project_id, directory, title, version, time_created, time_updated, revert) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`).run(
    'ses_rev_user', 'p1', '/work/p1', 'User Revert', '1.19.3', 1000, 1000, JSON.stringify({
      messageID: 'msg_u1',
      partID: 'prt_000002_cut',
    }),
  );
  db.prepare(`INSERT INTO message (id, session_id, time_created, time_updated, data) VALUES (?, ?, ?, ?, ?)`).run(
    'msg_u1', 'ses_rev_user', 1000, 1000, JSON.stringify({ role: 'user' }),
  );
  db.prepare(`INSERT INTO part VALUES (?, ?, ?, ?, ?, ?)`).run('prt_000001_keep', 'msg_u1', 'ses_rev_user', 1000, 1000, JSON.stringify({ type: 'text', text: 'keep' }));
  db.prepare(`INSERT INTO part VALUES (?, ?, ?, ?, ?, ?)`).run('prt_000002_cut', 'msg_u1', 'ses_rev_user', 1001, 1001, JSON.stringify({ type: 'text', text: 'cut' }));
  db.close();

  const provider = createOpencodeProvider({ rootDir: dir, homeDir: home, openDatabase: openDb });
  const units = provider.discover({ lastCursor: () => null });
  const records = [...provider.parse(units[0], null)];

  const msgs = records.filter(r => r.kind === 'message');
  assert.equal(msgs.length, 2);
  assert.equal(msgs[0].text, 'keep');
  assert.equal(msgs[0].visibility, 'visible');
  assert.equal(msgs[1].text, 'cut');
  assert.equal(msgs[1].visibility, 'inactive');

  const sessRecord = records.find(r => r.kind === 'session');
  assert.equal(sessRecord.message_count, 1, 'message_count only includes visible parts');
});

test('P2-8: V2 skill and shell project to canonical records, and unknown types are preserved without dropping history', () => {
  const home = makeTempDir('opencode-types-p28-');
  const dir = join(home, 'data');
  mkdirSync(dir, { recursive: true });
  const dbPath = join(dir, 'opencode.db');
  initTestStore(dbPath);

  const db = new DatabaseSync(dbPath);
  db.prepare(`INSERT INTO session_v2 (id, project_id, directory, title, version, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?, ?)`).run('ses_types', 'p1', '/work/p1', 'Types Test', '2.0.32', 1000, 1000);
  db.prepare(`INSERT INTO session_message (id, session_id, type, seq, time_created, time_updated, data) VALUES (?, ?, ?, ?, ?, ?, ?)`).run(
    'm1_skill', 'ses_types', 'skill', 1, 1000, 1000, JSON.stringify({ name: 'code-review', instructions: 'perform thorough review' }),
  );
  db.prepare(`INSERT INTO session_message (id, session_id, type, seq, time_created, time_updated, data) VALUES (?, ?, ?, ?, ?, ?, ?)`).run(
    'm2_shell', 'ses_types', 'shell', 2, 1001, 1001, JSON.stringify({ command: 'git status', status: 'completed', output: 'clean' }),
  );
  db.prepare(`INSERT INTO session_message (id, session_id, type, seq, time_created, time_updated, data) VALUES (?, ?, ?, ?, ?, ?, ?)`).run(
    'm3_future', 'ses_types', 'future-agent-action', 3, 1002, 1002, JSON.stringify({ text: 'future content' }),
  );
  db.close();

  const provider = createOpencodeProvider({ rootDir: dir, homeDir: home, openDatabase: openDb });
  const units = provider.discover({ lastCursor: () => null });
  const records = [...provider.parse(units[0], null)];

  const msgs = records.filter(r => r.kind === 'message');
  assert.equal(msgs.length, 3, 'all 3 non-standard messages emitted canonical records');

  // skill
  assert.equal(msgs[0].content_type, 'skill_instructions');
  assert.equal(msgs[0].text, 'perform thorough review');
  assert.equal(msgs[0].skill, 'code-review');

  // shell
  assert.equal(msgs[1].content_type, 'tool_use');
  const toolCall = records.find(r => r.kind === 'tool_call');
  assert.ok(toolCall);
  assert.equal(toolCall.name, 'bash');
  assert.ok(toolCall.input_json.includes('git status'));
  const toolRes = records.find(r => r.kind === 'tool_result');
  assert.ok(toolRes);
  assert.equal(toolRes.content, 'clean');

  // future type
  assert.equal(msgs[2].text, 'future content');
  assert.equal(msgs[2].is_meta, 1);
});

test('P2-9: OPENCODE_DB absolute path configures symmetric watchTargets, and new channel promotes in watcher filter without readdirSync', async () => {
  const home = makeTempDir('opencode-watch-p29-');
  const dir = join(home, 'data');
  mkdirSync(dir, { recursive: true });
  const customDbPath = join(home, 'isolated-other.db');
  writeFileSync(customDbPath, '');

  const prevEnv = process.env.OPENCODE_DB;
  try {
    process.env.OPENCODE_DB = customDbPath;
    const provider = createOpencodeProvider({ homeDir: home, openDatabase: openDb });
    const targets = provider.watchTargets(provider.descriptor.defaultRoot);

    // Watch targets point to customDbPath and customDbPath-wal, not home dir
    const filePaths = targets.filter(t => t.kind === 'file').map(t => t.path);
    assert.ok(filePaths.includes(customDbPath), 'watches custom OPENCODE_DB file');
    assert.ok(filePaths.includes(`${customDbPath}-wal`), 'watches custom OPENCODE_DB WAL');

    // Test caller filter matches newly created channel db
    delete process.env.OPENCODE_DB;
    const standardProvider = createOpencodeProvider({ rootDir: dir, homeDir: home, openDatabase: openDb });
    const dirTargets = standardProvider.watchTargets(dir);
    const treeTarget = dirTargets.find(t => t.kind === 'tree');
    assert.ok(treeTarget);
    assert.ok(typeof treeTarget.matches === 'function');
    assert.equal(treeTarget.matches('opencode-dev.db'), true);
    assert.equal(treeTarget.matches('opencode-dev.db-wal'), true);
    assert.equal(treeTarget.matches('opencode-dev.db-shm'), false);
    assert.equal(treeTarget.matches('random.txt'), false);
  } finally {
    if (prevEnv !== undefined) process.env.OPENCODE_DB = prevEnv; else delete process.env.OPENCODE_DB;
  }
});

test('P2-10: parent classification consistency: rechecked within snapshot, and orphan parent matches between parse and raw', () => {
  const home = makeTempDir('opencode-parent-p210-');
  const dir = join(home, 'data');
  mkdirSync(dir, { recursive: true });
  const dbPath = join(dir, 'opencode.db');
  initTestStore(dbPath);

  const db = new DatabaseSync(dbPath);
  // Orphan session: parent_id refers to non-existent parent 'missing_parent_id'
  db.prepare(`INSERT INTO session_v2 (id, project_id, directory, title, version, time_created, time_updated, parent_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`).run(
    'ses_orphan', 'p1', '/work/p1', 'Orphan Session', '2.0.32', 1000, 1000, 'missing_parent_id',
  );
  db.prepare(`INSERT INTO session_message (id, session_id, type, seq, time_created, time_updated, data) VALUES (?, ?, ?, ?, ?, ?, ?)`).run(
    'm1', 'ses_orphan', 'user', 1, 1000, 1000, JSON.stringify({ text: 'orphan text' }),
  );
  db.close();

  const provider = createOpencodeProvider({ rootDir: dir, homeDir: home, openDatabase: openDb });
  const units = provider.discover({ lastCursor: () => null });
  assert.equal(units[0].isSubagent, false, 'orphan session is not subagent because parent does not exist');

  const { cursor } = drain(provider.parse(units[0], null));
  assert.ok(cursor);

  // Raw lookup with freshly parsed cursor must succeed and not fail fingerprint
  const sessionId = opencodeSessionId('ses_orphan', dbPath);
  const jsonlPath = opencodeUnitKey(dbPath, 'ses_orphan');
  const uuid = `${sessionId}:m:000001:m1:msg`;

  const rawResult = provider.raw({
    source: 'opencode',
    messageUuid: uuid,
    cursor,
    session: { id: sessionId, jsonl_path: jsonlPath },
  });
  assert.ok(rawResult, 'raw succeeds and authenticates cursor for orphan session');
  assert.equal(rawResult.messageText, 'orphan text');
});

test('P2-11: deterministic V1 part ordering by upstream ID regardless of insertion order', () => {
  const home = makeTempDir('opencode-order-p211-');
  const dir = join(home, 'data');
  mkdirSync(dir, { recursive: true });
  const dbPath = join(dir, 'opencode.db');
  initTestStore(dbPath);

  const db = new DatabaseSync(dbPath);
  db.prepare(`INSERT INTO session (id, project_id, directory, title, version, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?, ?)`).run('ses_v1_order', 'p1', '/work/p1', 'Order', '1.19.3', 1000, 1000);
  db.prepare(`INSERT INTO message (id, session_id, time_created, time_updated, data) VALUES (?, ?, ?, ?, ?)`).run('msg_1', 'ses_v1_order', 1000, 1000, JSON.stringify({ role: 'assistant' }));

  // Reverse physical insertion: prt_000002 inserted before prt_000001 with SAME timestamp
  db.prepare(`INSERT INTO part VALUES (?, ?, ?, ?, ?, ?)`).run('prt_000002', 'msg_1', 'ses_v1_order', 1000, 1000, JSON.stringify({ type: 'text', text: 'second' }));
  db.prepare(`INSERT INTO part VALUES (?, ?, ?, ?, ?, ?)`).run('prt_000001', 'msg_1', 'ses_v1_order', 1000, 1000, JSON.stringify({ type: 'text', text: 'first' }));
  db.close();

  const provider = createOpencodeProvider({ rootDir: dir, homeDir: home, openDatabase: openDb });
  const units = provider.discover({ lastCursor: () => null });
  const msgs = [...provider.parse(units[0], null)].filter(r => r.kind === 'message');

  assert.equal(msgs.length, 2);
  assert.equal(msgs[0].text, 'first', 'first part emitted first');
  assert.equal(msgs[1].text, 'second', 'second part emitted second');
  assert.equal(msgs[1].parent_uuid, msgs[0].uuid, 'parent chain flows from first to second');
});
