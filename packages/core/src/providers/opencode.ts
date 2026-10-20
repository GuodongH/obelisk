// Copyright (C) 2026 tommy0103 and contributors.
// SPDX-License-Identifier: AGPL-3.0-only

// OpenCode provider adapter in Core (see docs/adr/0001, ADR-0006, ADR-0007).
//
// Pure: discovers OpenCode SQLite stores (V1 session/message/part and V2
// session[_v2]/session_message) and parses sessions into canonical records. It never
// writes source databases. SQLite opening is injected because Core runs under both
// node:sqlite (CLI) and better-sqlite3 (Electron).
//
// Source layout ($root = XDG_DATA_HOME || ~/.local/share/opencode by default, all platforms):
//   opencode.db          — standard authoritative store (WAL mode)
//   opencode-<channel>.db — channel-specific stores (e.g. opencode-next.db)
//
// Every mutation is handled by complete session snapshot replacement: when a
// unit's content fingerprint changed, retractSessionIds clears the old records and
// parse re-emits everything with countMode 'total'.
//
// Canonical tokens:
//   V2: input_tokens = input + cache.read + cache.write, output_tokens = output + reasoning
//   V1: step-finish per-step priority; assistant.tokens only when no step-finish occurred.

import { createHash } from 'node:crypto';
import { existsSync, readdirSync, statSync } from 'node:fs';
import { homedir } from 'node:os';
import { dirname, isAbsolute, join, normalize, sep } from 'node:path';

import { normalizeObservedCwd, projectSlugFromPath, trunc, truncJson } from '../parsing.ts';
import type { SqliteDb } from '../sqlite-types.ts';
import type {
  Cursor,
  DiscoverContext,
  IndexUnit,
  MessageRecord,
  MessageVisibility,
  ProviderAdapter,
  RawLookup,
  RawRecord,
  SessionRecord,
  SubagentRecord,
  SummaryRecord,
  TranscriptRecord,
  WatchTarget,
} from './types.ts';

export const name = 'opencode';
export const OPENCODE_CANONICAL_TRANSCRIPT_MARKER = '__opencode_canonical_transcript_v2__';

const TOMBSTONE_CURSOR = '0:0:v1:tombstone';

export type OpencodeDatabaseOpener = (dbPath: string) => SqliteDb;

type JsonRecord = Record<string, unknown>;

function isRecord(value: unknown): value is JsonRecord {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function parseJson(text: unknown): JsonRecord | null {
  if (typeof text !== 'string' || text.length === 0) return null;
  try {
    const value: unknown = JSON.parse(text);
    return isRecord(value) ? value : null;
  } catch {
    return null;
  }
}

function transcriptObject(text: unknown, location: string): JsonRecord {
  const value = parseJson(text);
  if (value === null) throw new Error(`opencode: invalid JSON payload at ${location}`);
  return value;
}

function modernMessageData(message: SessionMessageRow): JsonRecord {
  const data = transcriptObject(message.data, `session_message ${message.id}`);
  if (message.type === 'user' && typeof data.text !== 'string') {
    throw new Error(`opencode: invalid user payload at session_message ${message.id}`);
  }
  if (message.type === 'assistant') {
    if (!Array.isArray(data.content) || data.content.some(item => !isRecord(item)
      || typeof item.type !== 'string'
      || ((item.type === 'text' || item.type === 'reasoning') && typeof item.text !== 'string')
      || (item.type === 'tool' && (str(item.id) === null || str(item.name) === null
        || !isRecord(item.state) || typeof item.state.status !== 'string')))) {
      throw new Error(`opencode: invalid assistant payload at session_message ${message.id}`);
    }
  }
  // The pre-split writer records callID/output/time, with no status or exit.
  const preSplitShell = data.status === undefined && typeof data.callID === 'string'
    && typeof data.output === 'string' && isRecord(data.time) && typeof data.time.created === 'number';
  if (message.type === 'shell' && (typeof data.command !== 'string' || (!preSplitShell && typeof data.status !== 'string')
    || (data.exit !== undefined && (typeof data.exit !== 'number' || !Number.isFinite(data.exit)))
    || (data.output !== undefined && typeof data.output !== 'string'
      && (!isRecord(data.output) || typeof data.output.output !== 'string')))) {
    throw new Error(`opencode: invalid shell payload at session_message ${message.id}`);
  }
  return data;
}

function legacyMessageData(message: MessageRow): JsonRecord {
  const data = transcriptObject(message.data, `message ${message.id}`);
  if (data.role !== 'user' && data.role !== 'assistant') {
    throw new Error(`opencode: invalid role payload at message ${message.id}`);
  }
  return data;
}

function legacyPartData(part: PartRow): JsonRecord {
  const data = transcriptObject(part.data, `part ${part.id}`);
  if (typeof data.type !== 'string'
    || ((data.type === 'text' || data.type === 'reasoning') && typeof data.text !== 'string')) {
    throw new Error(`opencode: invalid part payload at part ${part.id}`);
  }
  return data;
}

function str(value: unknown): string | null {
  return typeof value === 'string' && value.length > 0 ? value : null;
}

export function parseModelRef(value: unknown): string | null {
  if (typeof value === 'string' && value.length > 0) {
    const parsed = parseJson(value);
    if (parsed && typeof parsed.id === 'string' && parsed.id.length > 0) {
      return parsed.id;
    }
    return value;
  }
  if (isRecord(value)) {
    if (typeof value.id === 'string' && value.id.length > 0) {
      return value.id;
    }
    if (typeof value.modelID === 'string' && value.modelID.length > 0) {
      return value.modelID;
    }
  }
  return null;
}

function checkIsSubagent(db: SqliteDb, parentId: string | null, tables: ReadonlySet<string>): boolean {
  if (parentId === null || parentId.length === 0) return false;
  return (['session_v2', 'session'] as const).some(table => tables.has(table)
    && db.prepare(`SELECT 1 FROM ${table} WHERE id = ?`).get(parentId) !== undefined);
}

const timestampOfMs = (time: number | null | undefined): string | null => (
  typeof time === 'number' && Number.isFinite(time) && time > 0 ? new Date(time).toISOString() : null
);

export function defaultOpencodeDataRoot(homeDir = homedir()): string {
  const xdg = process.env.XDG_DATA_HOME;
  if (xdg !== undefined && xdg.trim().length > 0) {
    return join(xdg.trim(), 'opencode');
  }
  return join(homeDir, '.local', 'share', 'opencode');
}

export function resolveOpencodeRoot(
  rootDir: string | undefined,
  homeDir: string = homedir(),
): { rootDir: string; singleDbFile?: string } {
  if (rootDir !== undefined && rootDir.trim().length > 0) {
    const expanded = rootDir.startsWith('~/') || rootDir.startsWith('~\\')
      ? join(homeDir, rootDir.slice(2))
      : normalize(rootDir);
    return { rootDir: expanded };
  }
  const envDb = process.env.OPENCODE_DB;
  if (envDb !== undefined && envDb.trim().length > 0 && envDb.trim() !== ':memory:') {
    const trimmed = envDb.trim();
    const storePath = isAbsolute(trimmed)
      ? normalize(trimmed)
      : normalize(join(defaultOpencodeDataRoot(homeDir), trimmed));
    return { rootDir: dirname(storePath), singleDbFile: storePath };
  }
  return { rootDir: defaultOpencodeDataRoot(homeDir) };
}

export function opencodeStoreHash(storePath: string): string {
  return createHash('sha256').update(`opencode-store-v1\0${normalize(storePath)}`).digest('hex');
}

export function opencodeSessionId(nativeId: string, storePath: string): string {
  return `opencode:${encodeURIComponent(nativeId)}:${opencodeStoreHash(storePath)}`;
}

export function opencodeUnitKey(storePath: string, nativeId: string): string {
  return `${normalize(storePath)}#ses:${encodeURIComponent(nativeId)}`;
}

function openSource(dbPath: string, opener: OpencodeDatabaseOpener): SqliteDb | null {
  try {
    return opener(dbPath);
  } catch {
    return null;
  }
}

function sourceTables(db: SqliteDb): Set<string> {
  try {
    return new Set(
      (db.prepare("SELECT name FROM sqlite_master WHERE type='table'").all() as Array<{ name: unknown }>)
        .map(r => String(r.name)),
    );
  } catch (cause) {
    throw new Error(`opencode: unable to read transcript tables: ${cause instanceof Error ? cause.message : String(cause)}`, { cause });
  }
}

export function isValidChannelDbName(filename: string): boolean {
  return /^opencode-[a-zA-Z0-9._-]+\.db$/.test(filename);
}

export function isValidChannelWalName(filename: string): boolean {
  return /^opencode-[a-zA-Z0-9._-]+\.db-wal$/.test(filename);
}

export function isOpencodeStoreOrWalFile(filename: string): boolean {
  return (
    filename === 'opencode.db'
    || filename === 'opencode.db-wal'
    || isValidChannelDbName(filename)
    || isValidChannelWalName(filename)
  );
}

function collectPriorStoresInDir(dir: string, ctx: DiscoverContext): string[] {
  const normDir = normalize(dir);
  const prefix = `${normDir}${sep}`;
  const indexedSessions = ctx.indexedSessions?.() ?? [];
  const stores = new Set<string>();
  for (const s of indexedSessions) {
    const hashIdx = s.jsonlPath.indexOf('#ses:');
    if (hashIdx !== -1) {
      const store = normalize(s.jsonlPath.slice(0, hashIdx));
      if (store.startsWith(prefix) || store === join(normDir, 'opencode.db')) {
        stores.add(store);
      }
    }
  }
  return Array.from(stores);
}

function discoverDatabaseFiles(dir: string): string[] {
  if (!existsSync(dir)) return [];
  try {
    const entries = readdirSync(dir, { withFileTypes: true });
    const paths: string[] = [];
    for (const entry of entries) {
      if (entry.isFile()) {
        if (entry.name === 'opencode.db' || isValidChannelDbName(entry.name)) {
          paths.push(join(dir, entry.name));
        }
      }
    }
    return paths.sort();
  } catch {
    return [];
  }
}

interface SessionRow {
  id: string;
  project_id: string | null;
  parent_id: string | null;
  directory: string | null;
  title: string | null;
  version: string | null;
  revert: string | null;
  cost: number | null;
  tokens_input: number | null;
  tokens_output: number | null;
  tokens_reasoning: number | null;
  tokens_cache_read: number | null;
  tokens_cache_write: number | null;
  time_created: number | null;
  time_updated: number | null;
  time_archived: number | null;
  agent: string | null;
  model: string | null;
}

interface SessionMessageRow {
  id: string;
  session_id: string;
  type: string;
  seq: number;
  time_created: number;
  time_updated: number;
  data: string;
}

interface MessageRow {
  id: string;
  session_id: string;
  time_created: number;
  time_updated: number;
  data: string;
}

interface PartRow {
  id: string;
  message_id: string;
  session_id: string;
  time_created: number;
  time_updated: number;
  data: string;
}

interface RevertInfo {
  messageID: string | null;
  partID: string | null;
}

function parseRevert(revert: string | null): RevertInfo | null {
  if (revert === null) return null;
  const value = transcriptObject(revert, 'session revert');
  if (typeof value.messageID !== 'string'
    || (value.partID !== undefined && value.partID !== null && typeof value.partID !== 'string')) {
    throw new Error('opencode: invalid revert payload');
  }
  return {
    messageID: str(value.messageID),
    partID: str(value.partID),
  };
}

function fingerprintSession(
  format: 'v1' | 'v2',
  session: SessionRow,
  isSubagent: boolean,
  v2Messages?: readonly SessionMessageRow[],
  v1Messages?: readonly MessageRow[],
  v1Parts?: readonly PartRow[],
): string {
  const hash = createHash('sha256');
  hash.update(JSON.stringify([
    format,
    session.id,
    session.parent_id,
    session.directory,
    session.title,
    session.version,
    session.revert,
    session.time_created,
    session.time_updated,
    session.time_archived,
    session.agent,
    session.model,
    isSubagent,
  ]));
  if (format === 'v2' && v2Messages) {
    for (const m of v2Messages) {
      hash.update(`\nm2${JSON.stringify([m.id, m.type, m.seq, m.time_created, m.time_updated, m.data])}`);
    }
  } else if (format === 'v1' && v1Messages && v1Parts) {
    for (const m of v1Messages) {
      hash.update(`\nm1${JSON.stringify([m.id, m.time_created, m.time_updated, m.data])}`);
    }
    for (const p of v1Parts) {
      hash.update(`\np1${JSON.stringify([p.id, p.message_id, p.time_created, p.time_updated, p.data])}`);
    }
  }
  return hash.digest('hex').slice(0, 24);
}

interface CursorState {
  timeUpdated: number;
  messageCount: number;
  fingerprint: string;
}

function encodeCursor(state: CursorState): string {
  return `${state.timeUpdated}:${state.messageCount}:v1:${state.fingerprint}`;
}

function decodeCursor(cursor: Cursor): CursorState | null {
  if (cursor === null) return null;
  const parts = cursor.split(':');
  if (parts.length !== 4 || parts[2] !== 'v1') return null;
  const timeUpdated = Number(parts[0]);
  const messageCount = Number(parts[1]);
  if (!Number.isFinite(timeUpdated) || !Number.isFinite(messageCount)) return null;
  return { timeUpdated, messageCount, fingerprint: parts[3]! };
}

interface OpencodeUnitMeta {
  kind: 'opencode-session';
  format: 'v1' | 'v2';
  dbPath: string;
  rawSessionId: string;
}

const FILE_TOOLS = new Set(['read', 'edit', 'write', 'apply_patch', 'patch']);

function toolFilePath(toolName: string, input: unknown): string | null {
  if (isRecord(input) && FILE_TOOLS.has(toolName.toLowerCase())) {
    if (typeof input.path === 'string' && input.path.length > 0) return input.path;
    if (typeof input.filePath === 'string' && input.filePath.length > 0) return input.filePath;
  }
  return null;
}

function v2UsageTokens(tokens: unknown): { input: number | null; output: number | null } {
  if (!isRecord(tokens)) return { input: null, output: null };
  const rawInput = tokens.input;
  const rawOutput = tokens.output;
  const rawReasoning = tokens.reasoning;
  const cache = isRecord(tokens.cache) ? tokens.cache : {};
  const cacheRead = typeof cache.read === 'number' && Number.isFinite(cache.read) ? cache.read : 0;
  const cacheWrite = typeof cache.write === 'number' && Number.isFinite(cache.write) ? cache.write : 0;

  let input: number | null = null;
  if (typeof rawInput === 'number' && Number.isFinite(rawInput)) {
    input = rawInput + cacheRead + cacheWrite;
  }
  let output: number | null = null;
  if (typeof rawOutput === 'number' && Number.isFinite(rawOutput)) {
    const reasoning = typeof rawReasoning === 'number' && Number.isFinite(rawReasoning) ? rawReasoning : 0;
    output = rawOutput + reasoning;
  }
  return { input, output };
}

function v1StepUsageTokens(tokens: unknown): { input: number | null; output: number | null } {
  if (!isRecord(tokens)) return { input: null, output: null };
  const rawInput = tokens.input;
  const rawOutput = tokens.output;
  const rawReasoning = tokens.reasoning;
  const cache = isRecord(tokens.cache) ? tokens.cache : {};
  const cacheRead = typeof cache.read === 'number' && Number.isFinite(cache.read) ? cache.read : 0;
  const cacheWrite = typeof cache.write === 'number' && Number.isFinite(cache.write) ? cache.write : 0;

  let input: number | null = null;
  if (typeof rawInput === 'number' && Number.isFinite(rawInput)) {
    input = rawInput + cacheRead + cacheWrite;
  }
  let output: number | null = null;
  if (typeof rawOutput === 'number' && Number.isFinite(rawOutput)) {
    const reasoning = typeof rawReasoning === 'number' && Number.isFinite(rawReasoning) ? rawReasoning : 0;
    output = rawOutput + reasoning;
  }
  return { input, output };
}

interface DiscoveredSessionEntry {
  format: 'v1' | 'v2';
  row: SessionRow;
}

type SessionTable = 'session_v2' | 'session';

function sessionSelect(table: SessionTable): string {
  // Metadata tables and wire formats have separate lifecycles: before the V2
  // split (v1.19.3), modern session_message rows also belonged to `session`.
  return `SELECT id, project_id, parent_id, directory, title, version, revert, cost,
    tokens_input, tokens_output, tokens_reasoning, tokens_cache_read, tokens_cache_write,
    time_created, time_updated, time_archived, agent, model FROM ${table}`;
}

function sessionFormat(
  db: SqliteDb,
  tables: ReadonlySet<string>,
  table: SessionTable,
  id: string,
  modernIds?: ReadonlySet<string>,
): 'v1' | 'v2' {
  const modern = tables.has('session_message');
  const legacy = tables.has('message') && tables.has('part');
  if (modern && (table === 'session_v2' || !legacy || (modernIds
    ? modernIds.has(id)
    : db.prepare('SELECT 1 FROM session_message WHERE session_id = ? LIMIT 1').get(id) !== undefined))) {
    return 'v2';
  }
  if (table === 'session' && legacy) return 'v1';
  throw new Error(`opencode: no recognized message tables for ${table} session ${id}`);
}

function readSessionEntry(db: SqliteDb, tables: ReadonlySet<string>, id: string): DiscoveredSessionEntry | undefined {
  for (const table of ['session_v2', 'session'] as const) {
    if (!tables.has(table)) continue;
    const row = db.prepare(`${sessionSelect(table)} WHERE id = ?`).get(id) as unknown as SessionRow | undefined;
    if (row !== undefined) return { row, format: sessionFormat(db, tables, table, id) };
  }
  return undefined;
}

function discoverAtStore(
  dbPath: string,
  ctx: DiscoverContext,
  opener: OpencodeDatabaseOpener,
): IndexUnit[] {
  const walPath = `${dbPath}-wal`;
  if (ctx.changedPaths !== undefined) {
    const normalized = normalize(dbPath);
    const touched = ctx.changedPaths.some((changed) => {
      const absolute = isAbsolute(changed) ? normalize(changed) : normalized;
      return absolute === normalized || absolute === normalize(walPath) || normalized.startsWith(absolute + sep);
    });
    if (!touched) return [];
  }

  let present: 'yes' | 'missing' | 'inaccessible';
  try {
    statSync(dbPath);
    present = 'yes';
  } catch (error) {
    const code = (error as NodeJS.ErrnoException).code;
    present = code === 'ENOENT' || code === 'ENOTDIR' ? 'missing' : 'inaccessible';
  }

  if (present === 'inaccessible') {
    ctx.reportIncompleteInventory?.({ path: dbPath, error: 'Source database is not accessible' });
    return [];
  }
  if (present === 'missing') {
    const prior = (ctx.indexedSessions?.() ?? []).some(s => s.jsonlPath.startsWith(`${normalize(dbPath)}#ses:`));
    if (prior) {
      ctx.reportIncompleteInventory?.({ path: dbPath, error: 'Source database is unavailable' });
    }
    return [];
  }

  const db = openSource(dbPath, opener);
  if (db === null) {
    ctx.reportIncompleteInventory?.({ path: dbPath, error: 'Session database is busy or unreadable' });
    return [];
  }

  try {
    db.exec('BEGIN');
    try {
      const tables = sourceTables(db);
      const hasModern = tables.has('session_message');
      const hasLegacy = tables.has('message') && tables.has('part');
      if ((!tables.has('session_v2') && !tables.has('session')) || (!hasModern && !hasLegacy)) {
        throw new Error('Session database has no recognized transcript tables');
      }

      const v2Counts = new Map<string, number>();
      if (hasModern) {
        for (const r of db.prepare(
          'SELECT session_id, COUNT(*) as c FROM session_message GROUP BY session_id',
        ).all() as unknown as Array<{ session_id: string; c: number }>) {
          v2Counts.set(r.session_id, r.c);
        }
      }
      const v1Counts = new Map<string, number>();
      if (hasLegacy) {
        for (const r of db.prepare(
          'SELECT session_id, COUNT(*) as c FROM message GROUP BY session_id',
        ).all() as unknown as Array<{ session_id: string; c: number }>) {
          v1Counts.set(r.session_id, r.c);
        }
      }

      const modernIds = new Set(v2Counts.keys());
      const sessionsMap = new Map<string, DiscoveredSessionEntry>();
      for (const table of ['session_v2', 'session'] as const) {
        if (!tables.has(table)) continue;
        for (const row of db.prepare(sessionSelect(table)).all() as unknown as SessionRow[]) {
          if (!sessionsMap.has(row.id)) {
            sessionsMap.set(row.id, { row, format: sessionFormat(db, tables, table, row.id, modernIds) });
          }
        }
      }

      const units: IndexUnit[] = [];
      const liveSessionIds = new Set<string>();
      const liveUnitKeys = new Set<string>();
      const liveRawIds = new Set(sessionsMap.keys());
      const indexedSessions = ctx.indexedSessions?.() ?? [];
      const indexedByKey = new Map<string, string[]>();
      for (const indexed of indexedSessions) {
        const ids = indexedByKey.get(indexed.jsonlPath) ?? [];
        ids.push(indexed.sessionId);
        indexedByKey.set(indexed.jsonlPath, ids);
      }

      // Depth-based sorting so parents process before children
      const depths = new Map<string, number>();
      const depth = (id: string, seen = new Set<string>()): number => {
        if (depths.has(id)) return depths.get(id)!;
        const parentId = sessionsMap.get(id)?.row.parent_id;
        if (!parentId || !sessionsMap.has(parentId) || seen.has(parentId)) return 0;
        seen.add(id);
        const value = depth(parentId, seen) + 1;
        depths.set(id, value);
        return value;
      };

      const sortedSessions = Array.from(sessionsMap.values()).sort(
        (a, b) => depth(a.row.id) - depth(b.row.id) || a.row.id.localeCompare(b.row.id),
      );

      const changedRawIds = new Set<string>();

      for (const entry of sortedSessions) {
        const session = entry.row;
        const sessionId = opencodeSessionId(session.id, dbPath);
        liveSessionIds.add(sessionId);
        const key = opencodeUnitKey(dbPath, session.id);
        liveUnitKeys.add(key);

        const replacedSessionIds = (indexedByKey.get(key) ?? []).filter(id => id !== sessionId);
        const count = entry.format === 'v2'
          ? (v2Counts.get(session.id) ?? 0)
          : (v1Counts.get(session.id) ?? 0);

        const isSubagent = session.parent_id !== null && liveRawIds.has(session.parent_id);
        const cursor = ctx.lastCursor(key);
        const state = decodeCursor(cursor);

        let changed = state === null || replacedSessionIds.length > 0;
        if (state !== null && !changed) {
          const l1Match = state.timeUpdated === (session.time_updated ?? 0)
            && state.messageCount === count;
          if (l1Match) {
            if (entry.format === 'v2') {
              const msgs = db.prepare(
                'SELECT id, session_id, type, seq, time_created, time_updated, data FROM session_message WHERE session_id = ? ORDER BY seq, time_created, id',
              ).all(session.id) as unknown as SessionMessageRow[];
              changed = fingerprintSession('v2', session, isSubagent, msgs) !== state.fingerprint;
            } else {
              const msgs = db.prepare(
                'SELECT id, session_id, time_created, time_updated, data FROM message WHERE session_id = ? ORDER BY time_created, id',
              ).all(session.id) as unknown as MessageRow[];
              const parts = db.prepare(
                'SELECT id, message_id, session_id, time_created, time_updated, data FROM part WHERE session_id = ? ORDER BY id',
              ).all(session.id) as unknown as PartRow[];
              changed = fingerprintSession('v1', session, isSubagent, undefined, msgs, parts) !== state.fingerprint;
            }
          } else {
            changed = true;
          }
        }

        if (isSubagent && session.parent_id !== null && changedRawIds.has(session.parent_id)) {
          changed = true;
        }

        if (!changed) continue;

        changedRawIds.add(session.id);
        units.push({
          key,
          sessionId,
          project: projectSlugFromPath(normalizeObservedCwd(session.directory)) ?? undefined,
          isSubagent,
          agentId: isSubagent ? sessionId : undefined,
          retractSessionIds: [sessionId, ...replacedSessionIds],
          meta: {
            kind: 'opencode-session',
            format: entry.format,
            dbPath,
            rawSessionId: session.id,
          } satisfies OpencodeUnitMeta,
        });
      }

      // Tombstones for deleted sessions in this store
      for (const indexed of indexedSessions) {
        if (!indexed.jsonlPath.startsWith(`${normalize(dbPath)}#ses:`)) continue;
        if (liveSessionIds.has(indexed.sessionId)) continue;
        if (liveUnitKeys.has(indexed.jsonlPath)) continue;
        units.push({
          key: indexed.jsonlPath,
          sessionId: indexed.sessionId,
          retractSessionIds: [indexed.sessionId],
          meta: {
            kind: 'opencode-session',
            format: 'v2',
            dbPath,
            rawSessionId: '',
          } satisfies OpencodeUnitMeta,
        });
      }

      return units;
    } finally {
      db.exec('COMMIT');
    }
  } catch (error) {
    ctx.reportIncompleteInventory?.({
      path: dbPath,
      error: error instanceof Error ? error.message : String(error),
    });
    return [];
  } finally {
    db.close();
  }
}

function discoverAll(
  resolvedRootDir: string | undefined,
  homeDir: string,
  ctx: DiscoverContext,
  opener: OpencodeDatabaseOpener,
): IndexUnit[] {
  // If explicit rootDir was configured, it takes priority over OPENCODE_DB
  if (resolvedRootDir !== undefined) {
    const dir = resolvedRootDir;
    let present: 'yes' | 'missing' | 'inaccessible';
    try {
      const st = statSync(dir);
      present = st.isDirectory() ? 'yes' : 'inaccessible';
    } catch (error) {
      const code = (error as NodeJS.ErrnoException).code;
      present = code === 'ENOENT' ? 'missing' : 'inaccessible';
    }

    if (present === 'inaccessible') {
      ctx.reportIncompleteInventory?.({ path: dir, error: 'Configured OpenCode directory is not accessible' });
      return [];
    }
    if (present === 'missing') {
      const prior = (ctx.indexedSessions?.() ?? []).some(s => s.jsonlPath.startsWith(`${normalize(dir)}${sep}`) || s.jsonlPath.startsWith(`${normalize(dir)}#ses:`));
      if (prior) {
        ctx.reportIncompleteInventory?.({ path: dir, error: 'Configured OpenCode directory is unavailable' });
      }
      return [];
    }

    const dbFiles = discoverDatabaseFiles(dir);
    const priorStores = collectPriorStoresInDir(dir, ctx);
    const allStores = Array.from(new Set([...dbFiles, ...priorStores])).sort();

    if (allStores.length === 0) {
      const prior = (ctx.indexedSessions?.() ?? []).some(s => s.jsonlPath.startsWith(`${normalize(dir)}${sep}`) || s.jsonlPath.startsWith(`${normalize(dir)}#ses:`));
      if (prior) {
        ctx.reportIncompleteInventory?.({ path: dir, error: 'Configured OpenCode directory contains no session databases' });
      }
      return [];
    }

    const units: IndexUnit[] = [];
    for (const dbPath of allStores) {
      units.push(...discoverAtStore(dbPath, ctx, opener));
    }
    return units;
  }

  // Automatic mode: check process.env.OPENCODE_DB
  const envDb = process.env.OPENCODE_DB;
  if (envDb !== undefined && envDb.trim().length > 0) {
    const trimmed = envDb.trim();
    if (trimmed === ':memory:') {
      ctx.reportIncompleteInventory?.({
        path: ':memory:',
        error: 'OpenCode in-memory database (:memory:) cannot be independently indexed',
      });
      return [];
    }
    const storePath = isAbsolute(trimmed)
      ? normalize(trimmed)
      : normalize(join(defaultOpencodeDataRoot(homeDir), trimmed));
    return discoverAtStore(storePath, ctx, opener);
  }

  // Default directory discovery
  const defaultDir = defaultOpencodeDataRoot(homeDir);
  if (!existsSync(defaultDir)) {
    const prior = (ctx.indexedSessions?.() ?? []).some(s => s.jsonlPath.startsWith(`${normalize(defaultDir)}${sep}`) || s.jsonlPath.startsWith(`${normalize(defaultDir)}#ses:`));
    if (prior) {
      ctx.reportIncompleteInventory?.({ path: defaultDir, error: 'OpenCode data directory is unavailable' });
    }
    return [];
  }

  const dbFiles = discoverDatabaseFiles(defaultDir);
  const priorStores = collectPriorStoresInDir(defaultDir, ctx);
  const allStores = Array.from(new Set([...dbFiles, ...priorStores])).sort();

  if (allStores.length === 0) {
    const prior = (ctx.indexedSessions?.() ?? []).some(s => s.jsonlPath.startsWith(`${normalize(defaultDir)}${sep}`) || s.jsonlPath.startsWith(`${normalize(defaultDir)}#ses:`));
    if (prior) {
      ctx.reportIncompleteInventory?.({ path: defaultDir, error: 'OpenCode data directory contains no session databases' });
    }
    return [];
  }

  const units: IndexUnit[] = [];
  for (const dbPath of allStores) {
    units.push(...discoverAtStore(dbPath, ctx, opener));
  }
  return units;
}

function* parseV2(
  unit: IndexUnit,
  meta: OpencodeUnitMeta,
  db: SqliteDb,
  session: SessionRow,
): Generator<TranscriptRecord, Cursor> {
  const sessionId = unit.sessionId;
  const isSubagent = unit.isSubagent === true;

  const messages = db.prepare(
    'SELECT id, session_id, type, seq, time_created, time_updated, data FROM session_message WHERE session_id = ? ORDER BY seq, time_created, id',
  ).all(meta.rawSessionId) as unknown as SessionMessageRow[];

  const fingerprint = fingerprintSession('v2', session, isSubagent, messages);
  const revert = parseRevert(session.revert);

  let activeCutIndex = -1;
  if (revert?.messageID) {
    activeCutIndex = messages.findIndex(m => m.id === revert.messageID);
  }

  const startedAt = timestampOfMs(session.time_created);
  let endedAt: string | null = null;
  const gitBranch: string | null = null;
  let visibleCount = 0;
  let previousMessageUuid: string | null = null;
  const records: TranscriptRecord[] = [];

  for (let mIdx = 0; mIdx < messages.length; mIdx++) {
    const m = messages[mIdx]!;
    const isMessageActive = activeCutIndex === -1 || mIdx < activeCutIndex;
    const baseVisibility: MessageVisibility = isMessageActive ? 'visible' : 'inactive';
    const messageTimestamp = timestampOfMs(m.time_created);
    const data = modernMessageData(m);

    if (isMessageActive && m.time_updated) {
      const updatedTime = timestampOfMs(m.time_updated);
      if (updatedTime !== null && (endedAt === null || updatedTime > endedAt)) {
        endedAt = updatedTime;
      }
    }

    if (m.type === 'compaction') {
      const status = str(data.status);
      const summaryText = str(data.summary);
      if (status === 'completed' && summaryText !== null) {
        const usage = v2UsageTokens(data.tokens);
        records.push({
          kind: 'summary',
          id: `${sessionId}:summary:${encodeURIComponent(m.id)}`,
          session_id: sessionId,
          timestamp: messageTimestamp,
          source: name,
          content: trunc(summaryText),
          visibility: baseVisibility,
          input_tokens: usage.input,
          output_tokens: usage.output,
        } satisfies SummaryRecord);
      } else {
        const usage = v2UsageTokens(data.tokens);
        if (usage.input !== null || usage.output !== null) {
          const uuid = `${sessionId}:m:${String(m.seq).padStart(6, '0')}:${encodeURIComponent(m.id)}:msg`;
          records.push({
            kind: 'message',
            uuid,
            session_id: sessionId,
            type: 'system',
            parent_uuid: previousMessageUuid,
            timestamp: messageTimestamp,
            role: 'system',
            text: null,
            content_type: 'unknown',
            is_meta: 1,
            visibility: 'hidden',
            model: parseModelRef(session.model) ?? null,
            is_sidechain: isSubagent ? 1 : 0,
            agent_id: isSubagent ? sessionId : null,
            input_tokens: usage.input,
            output_tokens: usage.output,
            cwd: session.directory ?? null,
            skill: null,
            source: name,
          } satisfies MessageRecord);
          previousMessageUuid = uuid;
        }
      }
      continue;
    }

    if (m.type === 'system' || m.type === 'synthetic') {
      const uuid = `${sessionId}:m:${String(m.seq).padStart(6, '0')}:${encodeURIComponent(m.id)}:msg`;
      const text = str(data.text);
      visibleCount += baseVisibility === 'visible' ? 1 : 0;
      records.push({
        kind: 'message',
        uuid,
        session_id: sessionId,
        type: 'system',
        parent_uuid: previousMessageUuid,
        timestamp: messageTimestamp,
        role: 'system',
        text: text !== null ? trunc(text) : null,
        content_type: text !== null ? 'text' : 'unknown',
        is_meta: 1,
        visibility: baseVisibility,
        model: parseModelRef(session.model) ?? null,
        is_sidechain: isSubagent ? 1 : 0,
        agent_id: isSubagent ? sessionId : null,
        input_tokens: null,
        output_tokens: null,
        cwd: session.directory ?? null,
        skill: null,
        source: name,
      } satisfies MessageRecord);
      previousMessageUuid = uuid;
      continue;
    }

    if (m.type === 'model-switched') {
      const uuid = `${sessionId}:m:${String(m.seq).padStart(6, '0')}:${encodeURIComponent(m.id)}:msg`;
      const toModel = parseModelRef(data.model);
      const fromModel = parseModelRef(data.previous);
      const cardText = fromModel !== null && toModel !== null
        ? `model ${fromModel} → ${toModel}`
        : `model → ${toModel ?? fromModel ?? 'default'}`;
      visibleCount += baseVisibility === 'visible' ? 1 : 0;
      records.push({
        kind: 'message',
        uuid,
        session_id: sessionId,
        type: 'system',
        parent_uuid: previousMessageUuid,
        timestamp: messageTimestamp,
        role: 'system',
        text: cardText,
        content_type: 'text',
        is_meta: 1,
        visibility: baseVisibility,
        model: toModel ?? parseModelRef(session.model) ?? null,
        is_sidechain: isSubagent ? 1 : 0,
        agent_id: isSubagent ? sessionId : null,
        input_tokens: null,
        output_tokens: null,
        cwd: session.directory ?? null,
        skill: null,
        source: name,
      } satisfies MessageRecord);
      previousMessageUuid = uuid;
      continue;
    }

    if (m.type === 'agent-switched') {
      const uuid = `${sessionId}:m:${String(m.seq).padStart(6, '0')}:${encodeURIComponent(m.id)}:msg`;
      const toAgent = str(data.agent);
      const fromAgent = str(data.previous);
      const cardText = fromAgent !== null && toAgent !== null
        ? `agent ${fromAgent} → ${toAgent}`
        : `agent → ${toAgent ?? fromAgent ?? 'default'}`;
      visibleCount += baseVisibility === 'visible' ? 1 : 0;
      records.push({
        kind: 'message',
        uuid,
        session_id: sessionId,
        type: 'system',
        parent_uuid: previousMessageUuid,
        timestamp: messageTimestamp,
        role: 'system',
        text: cardText,
        content_type: 'text',
        is_meta: 1,
        visibility: baseVisibility,
        model: parseModelRef(session.model) ?? null,
        is_sidechain: isSubagent ? 1 : 0,
        agent_id: isSubagent ? sessionId : null,
        input_tokens: null,
        output_tokens: null,
        cwd: session.directory ?? null,
        skill: null,
        source: name,
      } satisfies MessageRecord);
      previousMessageUuid = uuid;
      continue;
    }

    if (m.type === 'location-switched') {
      const uuid = `${sessionId}:m:${String(m.seq).padStart(6, '0')}:${encodeURIComponent(m.id)}:msg`;
      visibleCount += baseVisibility === 'visible' ? 1 : 0;
      records.push({
        kind: 'message',
        uuid,
        session_id: sessionId,
        type: 'system',
        parent_uuid: previousMessageUuid,
        timestamp: messageTimestamp,
        role: 'system',
        text: 'location switched',
        content_type: 'text',
        is_meta: 1,
        visibility: baseVisibility,
        model: parseModelRef(session.model) ?? null,
        is_sidechain: isSubagent ? 1 : 0,
        agent_id: isSubagent ? sessionId : null,
        input_tokens: null,
        output_tokens: null,
        cwd: session.directory ?? null,
        skill: null,
        source: name,
      } satisfies MessageRecord);
      previousMessageUuid = uuid;
      continue;
    }

    if (m.type === 'idle') {
      const uuid = `${sessionId}:m:${String(m.seq).padStart(6, '0')}:${encodeURIComponent(m.id)}:msg`;
      records.push({
        kind: 'message',
        uuid,
        session_id: sessionId,
        type: 'system',
        parent_uuid: previousMessageUuid,
        timestamp: messageTimestamp,
        role: 'system',
        text: null,
        content_type: 'unknown',
        is_meta: 1,
        visibility: 'hidden',
        model: parseModelRef(session.model) ?? null,
        is_sidechain: isSubagent ? 1 : 0,
        agent_id: isSubagent ? sessionId : null,
        input_tokens: null,
        output_tokens: null,
        cwd: session.directory ?? null,
        skill: null,
        source: name,
      } satisfies MessageRecord);
      previousMessageUuid = uuid;
      continue;
    }

    if (m.type === 'user') {
      const uuid = `${sessionId}:m:${String(m.seq).padStart(6, '0')}:${encodeURIComponent(m.id)}:msg`;
      const userText = str(data.text);
      visibleCount += baseVisibility === 'visible' ? 1 : 0;
      records.push({
        kind: 'message',
        uuid,
        session_id: sessionId,
        type: 'user',
        parent_uuid: previousMessageUuid,
        timestamp: messageTimestamp,
        role: 'user',
        text: userText !== null ? trunc(userText) : null,
        content_type: userText !== null ? 'text' : 'unknown',
        is_meta: 0,
        visibility: baseVisibility,
        model: null,
        is_sidechain: isSubagent ? 1 : 0,
        agent_id: isSubagent ? sessionId : null,
        input_tokens: null,
        output_tokens: null,
        cwd: session.directory ?? null,
        skill: null,
        source: name,
      } satisfies MessageRecord);
      previousMessageUuid = uuid;
      continue;
    }

    if (m.type === 'assistant') {
      const model = parseModelRef(data.model) ?? parseModelRef(session.model) ?? null;
      const content = Array.isArray(data.content) ? data.content : [];
      const assistantRecords: MessageRecord[] = [];
      let hasToolCall = false;

      for (let cIdx = 0; cIdx < content.length; cIdx++) {
        const item = content[cIdx];
        if (!isRecord(item)) continue;
        const itemType = str(item.type);
        const blockKey = String(cIdx).padStart(4, '0');

        if (itemType === 'text') {
          const textVal = str(item.text);
          if (textVal === null) continue;
          const uuid = `${sessionId}:m:${String(m.seq).padStart(6, '0')}:${encodeURIComponent(m.id)}:c:${blockKey}:text`;
          visibleCount += baseVisibility === 'visible' ? 1 : 0;
          const record: MessageRecord = {
            kind: 'message',
            uuid,
            session_id: sessionId,
            type: 'assistant',
            parent_uuid: previousMessageUuid,
            timestamp: messageTimestamp,
            role: 'assistant',
            text: trunc(textVal),
            content_type: 'text',
            is_meta: 0,
            visibility: baseVisibility,
            model,
            is_sidechain: isSubagent ? 1 : 0,
            agent_id: isSubagent ? sessionId : null,
            input_tokens: null,
            output_tokens: null,
            cwd: session.directory ?? null,
            skill: null,
            source: name,
          };
          assistantRecords.push(record);
          records.push(record);
          previousMessageUuid = uuid;
        } else if (itemType === 'reasoning') {
          const thinkVal = str(item.text);
          if (thinkVal === null) continue;
          const uuid = `${sessionId}:m:${String(m.seq).padStart(6, '0')}:${encodeURIComponent(m.id)}:c:${blockKey}:think`;
          visibleCount += baseVisibility === 'visible' ? 1 : 0;
          const record: MessageRecord = {
            kind: 'message',
            uuid,
            session_id: sessionId,
            type: 'assistant',
            parent_uuid: previousMessageUuid,
            timestamp: messageTimestamp,
            role: 'assistant',
            text: trunc(thinkVal),
            content_type: 'thinking',
            is_meta: 0,
            visibility: baseVisibility,
            model,
            is_sidechain: isSubagent ? 1 : 0,
            agent_id: isSubagent ? sessionId : null,
            input_tokens: null,
            output_tokens: null,
            cwd: session.directory ?? null,
            skill: null,
            source: name,
          };
          assistantRecords.push(record);
          records.push(record);
          previousMessageUuid = uuid;
        } else if (itemType === 'tool') {
          const callId = str(item.id);
          const toolName = str(item.name);
          if (callId === null || toolName === null) continue;
          hasToolCall = true;

          const uuid = `${sessionId}:m:${String(m.seq).padStart(6, '0')}:${encodeURIComponent(m.id)}:c:${blockKey}:tool`;
          const toolCallId = `${uuid}:call:${encodeURIComponent(callId)}`;
          const state = isRecord(item.state) ? item.state : {};
          const status = str(state.status);
          const input = state.input ?? {};
          const filePath = toolFilePath(toolName, input);

          visibleCount += baseVisibility === 'visible' ? 1 : 0;
          const record: MessageRecord = {
            kind: 'message',
            uuid,
            session_id: sessionId,
            type: 'assistant',
            parent_uuid: previousMessageUuid,
            timestamp: messageTimestamp,
            role: 'assistant',
            text: null,
            content_type: 'tool_use',
            is_meta: 0,
            visibility: baseVisibility,
            model,
            is_sidechain: isSubagent ? 1 : 0,
            agent_id: isSubagent ? sessionId : null,
            input_tokens: null,
            output_tokens: null,
            cwd: session.directory ?? null,
            skill: null,
            source: name,
          };
          assistantRecords.push(record);
          records.push(record);
          previousMessageUuid = uuid;

          records.push({
            kind: 'tool_call',
            id: toolCallId,
            message_uuid: uuid,
            session_id: sessionId,
            name: toolName,
            presentation: 'default',
            input_json: truncJson(input) ?? '{}',
            file_path: filePath,
          });

          if (status === 'completed' || status === 'error') {
            let contentStr = '';
            const isError: 0 | 1 = status === 'error' ? 1 : 0;
            if (status === 'error') {
              contentStr = typeof state.error === 'string'
                ? state.error
                : JSON.stringify(state.error ?? '');
            } else if (typeof state.content === 'string') {
              contentStr = state.content;
            } else if (Array.isArray(state.content)) {
              contentStr = state.content
                .map(c => (isRecord(c) && typeof c.text === 'string' ? c.text : JSON.stringify(c)))
                .join('\n');
            } else if (state.output !== undefined && state.output !== null) {
              contentStr = typeof state.output === 'string'
                ? state.output
                : JSON.stringify(state.output);
            }
            records.push({
              kind: 'tool_result',
              tool_use_id: toolCallId,
              message_uuid: uuid,
              session_id: sessionId,
              content: trunc(contentStr),
              file_path: filePath,
              is_error: isError,
            });
          }
        }
      }

      // If no text/think/tool messages emitted, emit textless anchor
      if (assistantRecords.length === 0) {
        const uuid = `${sessionId}:m:${String(m.seq).padStart(6, '0')}:${encodeURIComponent(m.id)}:msg`;
        visibleCount += baseVisibility === 'visible' ? 1 : 0;
        const record: MessageRecord = {
          kind: 'message',
          uuid,
          session_id: sessionId,
          type: 'assistant',
          parent_uuid: previousMessageUuid,
          timestamp: messageTimestamp,
          role: 'assistant',
          text: null,
          content_type: hasToolCall ? 'tool_use' : 'unknown',
          is_meta: 0,
          visibility: baseVisibility,
          model,
          is_sidechain: isSubagent ? 1 : 0,
          agent_id: isSubagent ? sessionId : null,
          input_tokens: null,
          output_tokens: null,
          cwd: session.directory ?? null,
          skill: null,
          source: name,
        };
        assistantRecords.push(record);
        records.push(record);
        previousMessageUuid = uuid;
      }

      // Attach token usage to the final assistant message record
      const usage = v2UsageTokens(data.tokens);
      if (assistantRecords.length > 0) {
        const lastRecord = assistantRecords.at(-1)!;
        lastRecord.input_tokens = usage.input;
        lastRecord.output_tokens = usage.output;
      }
      continue;
    }

    if (m.type === 'skill') {
      const uuid = `${sessionId}:m:${String(m.seq).padStart(6, '0')}:${encodeURIComponent(m.id)}:msg`;
      const skillName = str(data.name) ?? str(data.skill);
      const instructions = str(data.instructions) ?? str(data.text);
      visibleCount += baseVisibility === 'visible' ? 1 : 0;
      records.push({
        kind: 'message',
        uuid,
        session_id: sessionId,
        type: 'system',
        parent_uuid: previousMessageUuid,
        timestamp: messageTimestamp,
        role: 'system',
        text: instructions !== null ? trunc(instructions) : null,
        content_type: instructions !== null ? 'skill_instructions' : 'text',
        is_meta: 1,
        visibility: baseVisibility,
        model: parseModelRef(session.model) ?? null,
        is_sidechain: isSubagent ? 1 : 0,
        agent_id: isSubagent ? sessionId : null,
        input_tokens: null,
        output_tokens: null,
        cwd: session.directory ?? null,
        skill: skillName ?? null,
        source: name,
      } satisfies MessageRecord);
      previousMessageUuid = uuid;
      continue;
    }

    if (m.type === 'shell') {
      const uuid = `${sessionId}:m:${String(m.seq).padStart(6, '0')}:${encodeURIComponent(m.id)}:msg`;
      const command = str(data.command);
      const status = str(data.status);
      const output = typeof data.output === 'string' ? data.output
        : isRecord(data.output) && typeof data.output.output === 'string' ? data.output.output : '';
      const terminal = status === 'exited' || status === 'timeout' || status === 'killed'
        || status === 'completed' || status === 'error'
        || (status === null && isRecord(data.time) && typeof data.time.completed === 'number');
      const failed = status === 'timeout' || status === 'killed' || status === 'error'
        || (status === 'exited' && typeof data.exit === 'number' && data.exit !== 0);
      const toolCallId = `${uuid}:call:${encodeURIComponent(m.id)}`;
      const usage = v2UsageTokens(data.tokens);
      visibleCount += baseVisibility === 'visible' ? 1 : 0;
      records.push({
        kind: 'message',
        uuid,
        session_id: sessionId,
        type: 'assistant',
        parent_uuid: previousMessageUuid,
        timestamp: messageTimestamp,
        role: 'assistant',
        text: null,
        content_type: 'tool_use',
        is_meta: 0,
        visibility: baseVisibility,
        model: parseModelRef(session.model) ?? null,
        is_sidechain: isSubagent ? 1 : 0,
        agent_id: isSubagent ? sessionId : null,
        input_tokens: usage.input,
        output_tokens: usage.output,
        cwd: session.directory ?? null,
        skill: null,
        source: name,
      } satisfies MessageRecord);
      records.push({
        kind: 'tool_call',
        id: toolCallId,
        message_uuid: uuid,
        session_id: sessionId,
        name: 'bash',
        presentation: 'default',
        input_json: JSON.stringify({ command: command ?? '' }),
        file_path: null,
      });
      if (terminal || output.length > 0) {
        records.push({
          kind: 'tool_result',
          tool_use_id: toolCallId,
          message_uuid: uuid,
          session_id: sessionId,
          content: trunc(output),
          file_path: null,
          is_error: failed ? 1 : 0,
        });
      }
      previousMessageUuid = uuid;
      continue;
    }

    // Preserved audit record for unknown future-type
    const uuid = `${sessionId}:m:${String(m.seq).padStart(6, '0')}:${encodeURIComponent(m.id)}:msg`;
    const text = str(data.text) ?? (typeof m.data === 'string' && m.data.length > 0 ? m.data : null);
    visibleCount += baseVisibility === 'visible' ? 1 : 0;
    records.push({
      kind: 'message',
      uuid,
      session_id: sessionId,
      type: 'system',
      parent_uuid: previousMessageUuid,
      timestamp: messageTimestamp,
      role: 'system',
      text: text !== null ? trunc(text) : null,
      content_type: text !== null ? 'text' : 'unknown',
      is_meta: 1,
      visibility: baseVisibility,
      model: parseModelRef(session.model) ?? null,
      is_sidechain: isSubagent ? 1 : 0,
      agent_id: isSubagent ? sessionId : null,
      input_tokens: null,
      output_tokens: null,
      cwd: session.directory ?? null,
      skill: null,
      source: name,
    } satisfies MessageRecord);
    previousMessageUuid = uuid;
  }

  if (isSubagent && session.parent_id !== null) {
    records.push({
      kind: 'subagent',
      agent_id: sessionId,
      session_id: opencodeSessionId(session.parent_id, meta.dbPath),
      parent_tool_use_id: null,
      agent_type: session.agent ?? null,
      description: session.title ?? null,
      duration_ms: null,
      total_tokens: null,
    } satisfies SubagentRecord);
  }

  records.push({
    kind: 'session',
    id: sessionId,
    title: session.title ?? null,
    project: projectSlugFromPath(normalizeObservedCwd(session.directory)),
    started_at: startedAt,
    ended_at: endedAt ?? timestampOfMs(session.time_updated ?? session.time_created),
    git_branch: gitBranch,
    version: session.version ?? null,
    message_count: visibleCount,
    countMode: 'total',
    jsonl_path: unit.key,
    source: name,
  } satisfies SessionRecord);

  for (const record of records) yield record;

  return encodeCursor({
    timeUpdated: session.time_updated ?? 0,
    messageCount: messages.length,
    fingerprint,
  });
}

function* parseV1(
  unit: IndexUnit,
  meta: OpencodeUnitMeta,
  db: SqliteDb,
  session: SessionRow,
): Generator<TranscriptRecord, Cursor> {
  const sessionId = unit.sessionId;
  const isSubagent = unit.isSubagent === true;

  const messages = db.prepare(
    'SELECT id, session_id, time_created, time_updated, data FROM message WHERE session_id = ? ORDER BY time_created, id',
  ).all(meta.rawSessionId) as unknown as MessageRow[];

  const parts = db.prepare(
    'SELECT id, message_id, session_id, time_created, time_updated, data FROM part WHERE session_id = ? ORDER BY id',
  ).all(meta.rawSessionId) as unknown as PartRow[];

  const fingerprint = fingerprintSession('v1', session, isSubagent, undefined, messages, parts);
  const revert = parseRevert(session.revert);

  const partsByMessage = new Map<string, PartRow[]>();
  for (const part of parts) {
    const list = partsByMessage.get(part.message_id) ?? [];
    list.push(part);
    partsByMessage.set(part.message_id, list);
  }

  let revertMessageIndex = -1;
  if (revert?.messageID) {
    revertMessageIndex = messages.findIndex(m => m.id === revert.messageID);
  }

  const startedAt = timestampOfMs(session.time_created);
  let endedAt: string | null = null;
  const gitBranch: string | null = null;
  let visibleCount = 0;
  let previousMessageUuid: string | null = null;
  const records: TranscriptRecord[] = [];

  for (let mIdx = 0; mIdx < messages.length; mIdx++) {
    const m = messages[mIdx]!;
    const mData = legacyMessageData(m);
    const role = str(mData.role) ?? 'user';
    const messageTimestamp = timestampOfMs(m.time_created);
    const mParts = partsByMessage.get(m.id) ?? [];

    const isAtRevert = revertMessageIndex !== -1 && mIdx === revertMessageIndex;
    const isAfterRevert = revertMessageIndex !== -1 && mIdx > revertMessageIndex;

    if (!isAfterRevert && m.time_updated) {
      const updatedTime = timestampOfMs(m.time_updated);
      if (updatedTime !== null && (endedAt === null || updatedTime > endedAt)) {
        endedAt = updatedTime;
      }
    }

    if (role === 'user') {
      let revertPartFound = false;
      let userEmittedAny = false;
      for (let pIdx = 0; pIdx < mParts.length; pIdx++) {
        const p = mParts[pIdx]!;
        const pData = legacyPartData(p);
        let isPartActive = true;
        if (isAfterRevert) {
          isPartActive = false;
        } else if (isAtRevert) {
          if (revert?.partID === null) {
            isPartActive = false;
          } else if (p.id === revert?.partID || revertPartFound) {
            revertPartFound = true;
            isPartActive = false;
          }
        }
        const visibility: MessageVisibility = isPartActive ? 'visible' : 'inactive';
        const pType = str(pData.type);
        if (pType === 'text') {
          const textVal = str(pData.text);
          if (textVal === null) continue;
          const uuid = `${sessionId}:m:${String(mIdx).padStart(6, '0')}:${encodeURIComponent(m.id)}:p:${encodeURIComponent(p.id)}:text`;
          visibleCount += visibility === 'visible' ? 1 : 0;
          records.push({
            kind: 'message',
            uuid,
            session_id: sessionId,
            type: 'user',
            parent_uuid: previousMessageUuid,
            timestamp: messageTimestamp,
            role: 'user',
            text: trunc(textVal),
            content_type: 'text',
            is_meta: 0,
            visibility,
            model: null,
            is_sidechain: isSubagent ? 1 : 0,
            agent_id: isSubagent ? sessionId : null,
            input_tokens: null,
            output_tokens: null,
            cwd: session.directory ?? null,
            skill: null,
            source: name,
          } satisfies MessageRecord);
          previousMessageUuid = uuid;
          userEmittedAny = true;
        }
      }

      if (!userEmittedAny) {
        const isReverted = isAfterRevert || (isAtRevert && revert?.partID === null);
        const visibility: MessageVisibility = isReverted ? 'inactive' : 'visible';
        const uuid = `${sessionId}:m:${String(mIdx).padStart(6, '0')}:${encodeURIComponent(m.id)}:msg`;
        const userText = str(mData.text);
        visibleCount += visibility === 'visible' ? 1 : 0;
        records.push({
          kind: 'message',
          uuid,
          session_id: sessionId,
          type: 'user',
          parent_uuid: previousMessageUuid,
          timestamp: messageTimestamp,
          role: 'user',
          text: userText !== null ? trunc(userText) : null,
          content_type: userText !== null ? 'text' : 'unknown',
          is_meta: 0,
          visibility,
          model: null,
          is_sidechain: isSubagent ? 1 : 0,
          agent_id: isSubagent ? sessionId : null,
          input_tokens: null,
          output_tokens: null,
          cwd: session.directory ?? null,
          skill: null,
          source: name,
        } satisfies MessageRecord);
        previousMessageUuid = uuid;
      }
      continue;
    }

    if (role === 'assistant') {
      const model = parseModelRef(mData.modelID) ?? parseModelRef(mData.model) ?? parseModelRef(session.model) ?? null;
      let hasStepTokens = false;
      const assistantRecords: MessageRecord[] = [];
      let currentStepRecords: MessageRecord[] = [];
      let hasToolCall = false;

      let revertPartFound = false;

      for (let pIdx = 0; pIdx < mParts.length; pIdx++) {
        const p = mParts[pIdx]!;
        const pData = legacyPartData(p);

        let isPartActive = true;
        if (isAfterRevert) {
          isPartActive = false;
        } else if (isAtRevert) {
          if (revert?.partID === null) {
            isPartActive = false;
          } else if (p.id === revert?.partID || revertPartFound) {
            revertPartFound = true;
            isPartActive = false;
          }
        }
        const visibility: MessageVisibility = isPartActive ? 'visible' : 'inactive';
        const pType = str(pData.type);

        if (pType === 'step-start') {
          currentStepRecords = [];
        } else if (pType === 'text') {
          const textVal = str(pData.text);
          if (textVal === null) continue;
          const uuid = `${sessionId}:m:${String(mIdx).padStart(6, '0')}:${encodeURIComponent(m.id)}:p:${encodeURIComponent(p.id)}:text`;
          visibleCount += visibility === 'visible' ? 1 : 0;
          const record: MessageRecord = {
            kind: 'message',
            uuid,
            session_id: sessionId,
            type: 'assistant',
            parent_uuid: previousMessageUuid,
            timestamp: messageTimestamp,
            role: 'assistant',
            text: trunc(textVal),
            content_type: 'text',
            is_meta: 0,
            visibility,
            model,
            is_sidechain: isSubagent ? 1 : 0,
            agent_id: isSubagent ? sessionId : null,
            input_tokens: null,
            output_tokens: null,
            cwd: session.directory ?? null,
            skill: null,
            source: name,
          };
          assistantRecords.push(record);
          currentStepRecords.push(record);
          records.push(record);
          previousMessageUuid = uuid;
        } else if (pType === 'reasoning') {
          const thinkVal = str(pData.text);
          if (thinkVal === null) continue;
          const uuid = `${sessionId}:m:${String(mIdx).padStart(6, '0')}:${encodeURIComponent(m.id)}:p:${encodeURIComponent(p.id)}:think`;
          visibleCount += visibility === 'visible' ? 1 : 0;
          const record: MessageRecord = {
            kind: 'message',
            uuid,
            session_id: sessionId,
            type: 'assistant',
            parent_uuid: previousMessageUuid,
            timestamp: messageTimestamp,
            role: 'assistant',
            text: trunc(thinkVal),
            content_type: 'thinking',
            is_meta: 0,
            visibility,
            model,
            is_sidechain: isSubagent ? 1 : 0,
            agent_id: isSubagent ? sessionId : null,
            input_tokens: null,
            output_tokens: null,
            cwd: session.directory ?? null,
            skill: null,
            source: name,
          };
          assistantRecords.push(record);
          currentStepRecords.push(record);
          records.push(record);
          previousMessageUuid = uuid;
        } else if (pType === 'tool') {
          const callId = str(pData.callID);
          const toolName = str(pData.tool);
          if (callId === null || toolName === null) continue;
          hasToolCall = true;

          const uuid = `${sessionId}:m:${String(mIdx).padStart(6, '0')}:${encodeURIComponent(m.id)}:p:${encodeURIComponent(p.id)}:tool`;
          const toolCallId = `${uuid}:call:${encodeURIComponent(callId)}`;
          const state = isRecord(pData.state) ? pData.state : {};
          const status = str(state.status);
          const input = state.input ?? {};
          const filePath = toolFilePath(toolName, input);

          visibleCount += visibility === 'visible' ? 1 : 0;
          const record: MessageRecord = {
            kind: 'message',
            uuid,
            session_id: sessionId,
            type: 'assistant',
            parent_uuid: previousMessageUuid,
            timestamp: messageTimestamp,
            role: 'assistant',
            text: null,
            content_type: 'tool_use',
            is_meta: 0,
            visibility,
            model,
            is_sidechain: isSubagent ? 1 : 0,
            agent_id: isSubagent ? sessionId : null,
            input_tokens: null,
            output_tokens: null,
            cwd: session.directory ?? null,
            skill: null,
            source: name,
          };
          assistantRecords.push(record);
          currentStepRecords.push(record);
          previousMessageUuid = uuid;

          records.push(record);
          records.push({
            kind: 'tool_call',
            id: toolCallId,
            message_uuid: uuid,
            session_id: sessionId,
            name: toolName,
            presentation: 'default',
            input_json: truncJson(input) ?? '{}',
            file_path: filePath,
          });

          if (status === 'completed' || status === 'error') {
            let contentStr = '';
            const isError: 0 | 1 = status === 'error' ? 1 : 0;
            if (status === 'error') {
              contentStr = typeof state.error === 'string'
                ? state.error
                : JSON.stringify(state.error ?? '');
            } else if (state.output !== undefined && state.output !== null) {
              contentStr = typeof state.output === 'string'
                ? state.output
                : JSON.stringify(state.output);
            }
            records.push({
              kind: 'tool_result',
              tool_use_id: toolCallId,
              message_uuid: uuid,
              session_id: sessionId,
              content: trunc(contentStr),
              file_path: filePath,
              is_error: isError,
            });
          }
        } else if (pType === 'step-finish') {
          const stepUsage = v1StepUsageTokens(pData.tokens);
          if (stepUsage.input !== null || stepUsage.output !== null) {
            hasStepTokens = true;
            if (currentStepRecords.length > 0) {
              const target = currentStepRecords.at(-1)!;
              target.input_tokens = stepUsage.input;
              target.output_tokens = stepUsage.output;
            } else {
              const uuid = `${sessionId}:m:${String(mIdx).padStart(6, '0')}:${encodeURIComponent(m.id)}:p:${encodeURIComponent(p.id)}:step`;
              visibleCount += visibility === 'visible' ? 1 : 0;
              const record: MessageRecord = {
                kind: 'message',
                uuid,
                session_id: sessionId,
                type: 'assistant',
                parent_uuid: previousMessageUuid,
                timestamp: messageTimestamp,
                role: 'assistant',
                text: null,
                content_type: 'unknown',
                is_meta: 0,
                visibility,
                model,
                is_sidechain: isSubagent ? 1 : 0,
                agent_id: isSubagent ? sessionId : null,
                input_tokens: stepUsage.input,
                output_tokens: stepUsage.output,
                cwd: session.directory ?? null,
                skill: null,
                source: name,
              };
              assistantRecords.push(record);
              currentStepRecords.push(record);
              records.push(record);
              previousMessageUuid = uuid;
            }
          }
        }
      }

      // If no text/think/tool messages emitted, emit textless anchor
      if (assistantRecords.length === 0) {
        const uuid = `${sessionId}:m:${String(mIdx).padStart(6, '0')}:${encodeURIComponent(m.id)}:msg`;
        const isReverted = isAfterRevert || (isAtRevert && revert?.partID === null);
        const visibility: MessageVisibility = isReverted ? 'inactive' : 'visible';
        visibleCount += visibility === 'visible' ? 1 : 0;
        const record: MessageRecord = {
          kind: 'message',
          uuid,
          session_id: sessionId,
          type: 'assistant',
          parent_uuid: previousMessageUuid,
          timestamp: messageTimestamp,
          role: 'assistant',
          text: null,
          content_type: hasToolCall ? 'tool_use' : 'unknown',
          is_meta: 0,
          visibility,
          model,
          is_sidechain: isSubagent ? 1 : 0,
          agent_id: isSubagent ? sessionId : null,
          input_tokens: null,
          output_tokens: null,
          cwd: session.directory ?? null,
          skill: null,
          source: name,
        };
        assistantRecords.push(record);
        records.push(record);
        previousMessageUuid = uuid;
      }

      // If no step-finish occurred, fallback to assistant.tokens
      if (!hasStepTokens && assistantRecords.length > 0) {
        const fallbackUsage = v1StepUsageTokens(mData.tokens);
        const lastRecord = assistantRecords.at(-1)!;
        lastRecord.input_tokens = fallbackUsage.input;
        lastRecord.output_tokens = fallbackUsage.output;
      }
    }
  }

  if (isSubagent && session.parent_id !== null) {
    records.push({
      kind: 'subagent',
      agent_id: sessionId,
      session_id: opencodeSessionId(session.parent_id, meta.dbPath),
      parent_tool_use_id: null,
      agent_type: session.agent ?? null,
      description: session.title ?? null,
      duration_ms: null,
      total_tokens: null,
    } satisfies SubagentRecord);
  }

  records.push({
    kind: 'session',
    id: sessionId,
    title: session.title ?? null,
    project: projectSlugFromPath(normalizeObservedCwd(session.directory)),
    started_at: startedAt,
    ended_at: endedAt ?? timestampOfMs(session.time_updated ?? session.time_created),
    git_branch: gitBranch,
    version: session.version ?? null,
    message_count: visibleCount,
    countMode: 'total',
    jsonl_path: unit.key,
    source: name,
  } satisfies SessionRecord);

  for (const record of records) yield record;

  return encodeCursor({
    timeUpdated: session.time_updated ?? 0,
    messageCount: messages.length,
    fingerprint,
  });
}

function* parse(
  unit: IndexUnit,
  _cursor: Cursor,
  opener: OpencodeDatabaseOpener,
): Generator<TranscriptRecord, Cursor> {
  const meta = unit.meta as OpencodeUnitMeta;
  if (!meta || meta.kind !== 'opencode-session') {
    throw new Error('opencode: invalid unit metadata');
  }

  if (meta.rawSessionId === '') {
    // Tombstone verification
    const db = openSource(meta.dbPath, opener);
    if (db === null) {
      throw new Error(`opencode: session database is busy or unreadable during tombstone verification: ${meta.dbPath}`);
    }
    try {
      const tables = sourceTables(db);
      if (!tables.has('session_v2') && !tables.has('session')) {
        throw new Error(`opencode: session database has no recognized transcript tables during tombstone verification: ${meta.dbPath}`);
      }
      const hashIndex = unit.key.indexOf('#ses:');
      const nativeId = hashIndex !== -1
        ? decodeURIComponent(unit.key.slice(hashIndex + 5))
        : decodeURIComponent(unit.sessionId.split(':')[1] ?? '');
      if (!nativeId) {
        throw new Error(`opencode: could not determine native session ID for tombstone: ${unit.key}`);
      }
      let exists = false;
      if (tables.has('session_v2')) {
        exists = db.prepare('SELECT 1 FROM session_v2 WHERE id = ?').get(nativeId) !== undefined;
      }
      if (!exists && tables.has('session')) {
        exists = db.prepare('SELECT 1 FROM session WHERE id = ?').get(nativeId) !== undefined;
      }
      if (exists) {
        throw new Error(`opencode: session still present during tombstone parse: ${unit.sessionId}`);
      }
      return TOMBSTONE_CURSOR;
    } finally {
      db.close();
    }
  }

  const db = openSource(meta.dbPath, opener);
  if (db === null) {
    throw new Error(`opencode: session database is busy or unreadable: ${meta.dbPath}`);
  }

  try {
    db.exec('BEGIN');
    try {
      const tables = sourceTables(db);
      const entry = readSessionEntry(db, tables, meta.rawSessionId);
      if (entry === undefined) {
        throw new Error(`opencode: session vanished before parse: ${meta.rawSessionId}`);
      }
      const { row: session, format } = entry;
      const verifiedIsSubagent = checkIsSubagent(db, session.parent_id, tables);
      if (verifiedIsSubagent !== (unit.isSubagent === true)) {
        throw new Error(`opencode: session parent classification changed during parse: ${session.id}`);
      }

      if (format === 'v2') {
        return yield* parseV2(unit, meta, db, session);
      } else {
        return yield* parseV1(unit, meta, db, session);
      }
    } finally {
      db.exec('COMMIT');
    }
  } finally {
    db.close();
  }
}

function raw(input: RawLookup, opener: OpencodeDatabaseOpener): RawRecord | null {
  if (input.source !== name || !input.messageUuid) return null;
  const jsonlPath = typeof input.session?.jsonl_path === 'string'
    ? input.session.jsonl_path
    : null;
  if (jsonlPath === null) return null;

  const hashIndex = jsonlPath.indexOf('#ses:');
  if (hashIndex === -1) return null;
  const dbPath = jsonlPath.slice(0, hashIndex);
  const rawSessionId = decodeURIComponent(jsonlPath.slice(hashIndex + 5));

  // Store & Session identity strict authentication
  const expectedSessionId = opencodeSessionId(rawSessionId, dbPath);
  const sessionId = typeof input.session?.id === 'string' ? input.session.id : '';
  if (sessionId !== expectedSessionId) {
    return null;
  }
  if (!input.messageUuid.startsWith(`${expectedSessionId}:`)) {
    return null;
  }

  const suffix = input.messageUuid.slice(expectedSessionId.length + 1);

  const db = openSource(dbPath, opener);
  if (db === null) return null;

  try {
    db.exec('BEGIN');
    try {
      const tables = sourceTables(db);
      const entry = readSessionEntry(db, tables, rawSessionId);
      if (entry === undefined) return null;
      const { row: sessionRow, format: sessionFormat } = entry;
      const isSubagent = checkIsSubagent(db, sessionRow.parent_id, tables);

      // Verify cursor if supplied
      if (input.cursor !== undefined && input.cursor !== null) {
        let calculatedCursor: string;
        if (sessionFormat === 'v2') {
          const msgs = db.prepare(
            'SELECT id, session_id, type, seq, time_created, time_updated, data FROM session_message WHERE session_id = ? ORDER BY seq, time_created, id',
          ).all(rawSessionId) as unknown as SessionMessageRow[];
          const fp = fingerprintSession('v2', sessionRow, isSubagent, msgs);
          calculatedCursor = encodeCursor({
            timeUpdated: sessionRow.time_updated ?? 0,
            messageCount: msgs.length,
            fingerprint: fp,
          });
        } else {
          const msgs = db.prepare(
            'SELECT id, session_id, time_created, time_updated, data FROM message WHERE session_id = ? ORDER BY time_created, id',
          ).all(rawSessionId) as unknown as MessageRow[];
          const parts = db.prepare(
            'SELECT id, message_id, session_id, time_created, time_updated, data FROM part WHERE session_id = ? ORDER BY id',
          ).all(rawSessionId) as unknown as PartRow[];
          const fp = fingerprintSession('v1', sessionRow, isSubagent, undefined, msgs, parts);
          calculatedCursor = encodeCursor({
            timeUpdated: sessionRow.time_updated ?? 0,
            messageCount: msgs.length,
            fingerprint: fp,
          });
        }
        if (input.cursor !== calculatedCursor) {
          return null;
        }
      }

      let projection: Record<string, unknown> | null = null;
      let messageText: string | null = null;

      if (sessionFormat === 'v2' && tables.has('session_message')) {
        if (suffix.startsWith('summary:')) {
          const rawMsgId = decodeURIComponent(suffix.slice(8));
          const row = db.prepare(
            'SELECT id, session_id, type, seq, time_created, time_updated, data FROM session_message WHERE session_id = ? AND id = ?',
          ).get(rawSessionId, rawMsgId) as unknown as SessionMessageRow | undefined;
          if (!row || row.type !== 'compaction') return null;
          const parsedData = modernMessageData(row);
          if (str(parsedData.status) !== 'completed' || str(parsedData.summary) === null) return null;
          projection = {
            format: 'v2',
            message: {
              id: row.id,
              session_id: row.session_id,
              type: row.type,
              seq: row.seq,
              time_created: row.time_created,
              time_updated: row.time_updated,
              data: parsedData,
            },
          };
          messageText = str(parsedData.summary);
        } else {
          // Format: m:(\d{6}):([^:]+)(:msg|:c:(\d{4}):(text|think|tool))
          const match = /^m:(\d{6}):([^:]+)(.*)$/.exec(suffix);
          if (!match) return null;
          const seq = Number(match[1]);
          const rawMsgId = decodeURIComponent(match[2]!);
          const rest = match[3]!;

          const row = db.prepare(
            'SELECT id, session_id, type, seq, time_created, time_updated, data FROM session_message WHERE session_id = ? AND id = ?',
          ).get(rawSessionId, rawMsgId) as unknown as SessionMessageRow | undefined;
          if (!row || row.seq !== seq) return null;

          const parsedData = modernMessageData(row);
          projection = {
            format: 'v2',
            message: {
              id: row.id,
              session_id: row.session_id,
              type: row.type,
              seq: row.seq,
              time_created: row.time_created,
              time_updated: row.time_updated,
              data: parsedData,
            },
          };

          if (rest === ':msg') {
            if (row.type === 'user') {
              messageText = str(parsedData.text);
            } else if (row.type === 'system' || row.type === 'synthetic') {
              messageText = str(parsedData.text);
            } else if (row.type === 'model-switched') {
              const toModel = parseModelRef(parsedData.model);
              const fromModel = parseModelRef(parsedData.previous);
              messageText = fromModel !== null && toModel !== null
                ? `model ${fromModel} → ${toModel}`
                : `model → ${toModel ?? fromModel ?? 'default'}`;
            } else if (row.type === 'agent-switched') {
              const toAgent = str(parsedData.agent);
              const fromAgent = str(parsedData.previous);
              messageText = fromAgent !== null && toAgent !== null
                ? `agent ${fromAgent} → ${toAgent}`
                : `agent → ${toAgent ?? fromAgent ?? 'default'}`;
            } else if (row.type === 'location-switched') {
              messageText = 'location switched';
            } else if (row.type === 'idle') {
              messageText = null;
            } else if (row.type === 'skill') {
              messageText = str(parsedData.instructions) ?? str(parsedData.text);
            } else if (row.type === 'assistant') {
              messageText = null;
            } else {
              messageText = str(parsedData.text) ?? null;
            }
          } else {
            const blockMatch = /^:c:(\d{4}):(text|think|tool)$/.exec(rest);
            if (!blockMatch) return null;
            if (row.type !== 'assistant') return null;
            const blockIdx = Number(blockMatch[1]);
            const blockKind = blockMatch[2]!;
            const content = Array.isArray(parsedData.content) ? parsedData.content : [];
            if (blockIdx < 0 || blockIdx >= content.length) return null;
            const block = content[blockIdx];
            if (!isRecord(block)) return null;

            if (blockKind === 'text') {
              if (block.type !== 'text') return null;
              messageText = str(block.text);
              if (messageText === null) return null;
            } else if (blockKind === 'think') {
              if (block.type !== 'reasoning') return null;
              messageText = str(block.text);
              if (messageText === null) return null;
            } else if (blockKind === 'tool') {
              if (block.type !== 'tool') return null;
              messageText = null;
            } else {
              return null;
            }
          }
        }
      } else if (sessionFormat === 'v1' && tables.has('message')) {
        // V1 suffix: m:(\d{6}):([^:]+)(:msg|:p:([^:]+):(text|think|tool|step))
        const match = /^m:(\d{6}):([^:]+)(.*)$/.exec(suffix);
        if (!match) return null;
        const ordinal = Number(match[1]);
        const rawMsgId = decodeURIComponent(match[2]!);
        const rest = match[3]!;

        const allV1Msgs = db.prepare(
          'SELECT id, session_id, time_created, time_updated, data FROM message WHERE session_id = ? ORDER BY time_created, id',
        ).all(rawSessionId) as unknown as MessageRow[];
        if (ordinal < 0 || ordinal >= allV1Msgs.length) return null;
        const mRow = allV1Msgs[ordinal]!;
        if (mRow.id !== rawMsgId) return null;

        const parts = db.prepare(
          'SELECT id, message_id, session_id, time_created, time_updated, data FROM part WHERE session_id = ? AND message_id = ? ORDER BY id',
        ).all(rawSessionId, rawMsgId) as unknown as PartRow[];

        const parsedMData = legacyMessageData(mRow);
        const parsedParts = parts.map(p => ({
          id: p.id,
          time_created: p.time_created,
          data: legacyPartData(p),
        }));

        projection = {
          format: 'v1',
          message: {
            id: mRow.id,
            session_id: mRow.session_id,
            time_created: mRow.time_created,
            time_updated: mRow.time_updated,
            data: parsedMData,
          },
          parts: parsedParts,
        };

        if (rest === ':msg') {
          if (parsedMData.role === 'user') {
            const textParts = parsedParts
              .filter(p => p.data.type === 'text' && typeof p.data.text === 'string')
              .map(p => p.data.text as string);
            messageText = textParts.length > 0 ? textParts.join('\n') : str(parsedMData.text);
          } else {
            messageText = null;
          }
        } else {
          const partMatch = /^:p:([^:]+):(text|think|tool|step)$/.exec(rest);
          if (!partMatch) return null;
          const targetPartId = decodeURIComponent(partMatch[1]!);
          const partKind = partMatch[2]!;
          const targetPart = parsedParts.find(p => p.id === targetPartId);
          if (!targetPart) return null;

          if (partKind === 'text') {
            if (targetPart.data.type !== 'text') return null;
            messageText = str(targetPart.data.text);
            if (messageText === null) return null;
          } else if (partKind === 'think') {
            if (targetPart.data.type !== 'reasoning') return null;
            messageText = str(targetPart.data.text);
            if (messageText === null) return null;
          } else if (partKind === 'tool') {
            if (targetPart.data.type !== 'tool') return null;
            messageText = null;
          } else if (partKind === 'step') {
            if (targetPart.data.type !== 'step-finish') return null;
            messageText = null;
          } else {
            return null;
          }
        }
      }

      if (projection === null) return null;

      const text = JSON.stringify(projection, null, 1);
      return {
        text,
        totalLength: text.length,
        hasMore: false,
        messageText: messageText || null,
      };
    } finally {
      db.exec('COMMIT');
    }
  } catch {
    return null;
  } finally {
    db.close();
  }
}

export function createOpencodeProvider({
  rootDir,
  homeDir = homedir(),
  openDatabase,
}: {
  rootDir?: string;
  homeDir?: string;
  openDatabase?: OpencodeDatabaseOpener;
} = {}): ProviderAdapter {
  const fallbackOpener: OpencodeDatabaseOpener = () => {
    throw new Error('No OpencodeDatabaseOpener injected');
  };
  const opener = openDatabase ?? fallbackOpener;

  const defaultDir = defaultOpencodeDataRoot(homeDir);
  const resolvedRoot = rootDir !== undefined && rootDir.trim().length > 0
    ? (rootDir.startsWith('~/') || rootDir.startsWith('~\\')
      ? join(homeDir, rootDir.slice(2))
      : normalize(rootDir))
    : undefined;

  return {
    name,
    descriptor: {
      id: name,
      name: 'OpenCode',
      vendor: 'Anomaly',
      defaultRoot: resolvedRoot ?? defaultDir,
      color: '#0ea5e9',
    },
    indexVersionMarker: OPENCODE_CANONICAL_TRANSCRIPT_MARKER,
    sessionUnitKey: (session) => session.jsonlPath,
    watchHints: (unitKey) => {
      const delimiter = unitKey.lastIndexOf('#ses:');
      if (delimiter < 1) return [];
      const store = unitKey.slice(0, delimiter);
      if (resolvedRoot === undefined && process.env.OPENCODE_DB?.trim() === ':memory:') return [];
      const scope = resolveOpencodeRoot(resolvedRoot, homeDir);
      if (scope.singleDbFile !== undefined
        ? normalize(store) !== normalize(scope.singleDbFile)
        : normalize(dirname(store)) !== normalize(scope.rootDir)) return [];
      return [store, `${store}-wal`];
    },
    discover: (ctx: DiscoverContext) => discoverAll(resolvedRoot, homeDir, ctx, opener),
    parse: (unit, cursor) => parse(unit, cursor, opener),
    watchTargets: (configuredRoot: string): WatchTarget[] => {
      const isDefault = resolvedRoot === undefined && (!configuredRoot || configuredRoot.trim().length === 0 || normalize(configuredRoot) === normalize(defaultDir));
      if (isDefault) {
        const envDb = process.env.OPENCODE_DB;
        if (envDb !== undefined && envDb.trim().length > 0 && envDb.trim() !== ':memory:') {
          const trimmed = envDb.trim();
          const storePath = isAbsolute(trimmed)
            ? normalize(trimmed)
            : normalize(join(defaultDir, trimmed));
          return [
            { kind: 'file', path: storePath },
            { kind: 'file', path: `${storePath}-wal` },
          ];
        }
      }
      const root = !isDefault ? normalize(configuredRoot) : defaultDir;
      const treeTarget: WatchTarget = {
        kind: 'tree',
        path: root,
        fileNames: ['opencode-next.db', 'opencode-next.db-wal', 'opencode.db', 'opencode.db-wal'],
      };
      Object.defineProperty(treeTarget, 'matches', {
        value: isOpencodeStoreOrWalFile,
        enumerable: false,
        writable: true,
        configurable: true,
      });
      return [
        { kind: 'file', path: join(root, 'opencode.db') },
        { kind: 'file', path: join(root, 'opencode.db-wal') },
        treeTarget,
      ];
    },
    raw: (input) => raw(input, opener),
  };
}
