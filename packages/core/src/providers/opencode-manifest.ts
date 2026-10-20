// Copyright (C) 2026 tommy0103 and contributors.
// SPDX-License-Identifier: AGPL-3.0-only

import { createHash } from 'node:crypto';
import { statSync } from 'node:fs';
import type { Cursor } from './types.ts';

export const digest = (value: unknown): string => createHash('sha256')
  .update(JSON.stringify(value)).digest('hex');

export interface StoreObservation {
  /** Physical DB identity plus relevant SQL schema; WAL generation is not DB identity. */
  identity: string;
  stamp: string;
  schema: string;
}

export interface OpencodeCursorState {
  manifest: string;
  recovery: number;
  store: StoreObservation;
  fingerprint: string;
}

export function storePhysicalState(dbPath: string): { identity: string; stamp: string } {
  const main = statSync(dbPath, { bigint: true });
  let wal;
  try { wal = statSync(`${dbPath}-wal`, { bigint: true }); }
  catch (error) {
    if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error;
  }
  const fields = (s: typeof main | undefined) => s === undefined ? null :
    [s.dev, s.ino, s.birthtimeNs, s.mtimeNs, s.ctimeNs, s.size].map(String);
  return {
    identity: digest([String(main.dev), String(main.ino), String(main.birthtimeNs)]),
    stamp: digest([fields(main), fields(wal)]),
  };
}

// The first two slots are shared index_state compatibility fields. Everything
// after the tag belongs to this adapter. Never interpret an old/unknown cursor
// as evidence that a source is unchanged.
export function encodeCursor(timeUpdated: number, state: OpencodeCursorState): string {
  return `${timeUpdated}:0:opencode-manifest-v1:${Buffer.from(JSON.stringify(state)).toString('base64url')}`;
}

export function decodeCursor(cursor: Cursor): OpencodeCursorState | null {
  if (cursor === null) return null;
  const parts = cursor.split(':');
  if (parts.length !== 4 || parts[2] !== 'opencode-manifest-v1') return null;
  try {
    const value = JSON.parse(Buffer.from(parts[3]!, 'base64url').toString('utf8'));
    const hash = (v: unknown) => typeof v === 'string' && /^[a-f0-9]{64}$/.test(v);
    if (!Number.isSafeInteger(value?.recovery) || value.recovery < 0 || !hash(value?.manifest) || !hash(value?.store?.identity) || !hash(value?.store?.stamp)
      || !hash(value?.store?.schema) || typeof value?.fingerprint !== 'string'
      || !/^[a-f0-9]{24}$/.test(value.fingerprint)) return null;
    return value as OpencodeCursorState;
  } catch { return null; }
}
