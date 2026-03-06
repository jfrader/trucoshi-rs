#!/usr/bin/env node

import { spawnSync } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

function exists(p) {
  try {
    fs.accessSync(p, fs.constants.X_OK);
    return true;
  } catch {
    return false;
  }
}

function resolveCargo() {
  // Allow explicit override.
  if (process.env.CARGO && process.env.CARGO.trim()) return process.env.CARGO.trim();

  // Prefer PATH cargo.
  const probe = spawnSync('cargo', ['--version'], { stdio: 'ignore' });
  if (probe.status === 0) return 'cargo';

  // Common rustup install locations.
  const home = os.homedir();
  const candidates = [
    path.join(home, '.cargo', 'bin', process.platform === 'win32' ? 'cargo.exe' : 'cargo'),
    // Some environments may only have rustup, but expose cargo as a symlink.
    path.join(home, '.cargo', 'bin', process.platform === 'win32' ? 'rustup.exe' : 'rustup'),
  ];

  for (const c of candidates) {
    if (exists(c)) return c;
  }

  return null;
}

const cargo = resolveCargo();
if (!cargo) {
  console.error(
    'cargo not found. Install Rust (rustup) and ensure cargo is on PATH, or set $CARGO.'
  );
  process.exit(1);
}

// Support: `node scripts/gen-ws-schema.mjs -- ./schemas/ws/v2`
const idx = process.argv.indexOf('--');
const outDir = idx === -1 ? './schemas/ws/v2' : (process.argv[idx + 1] ?? './schemas/ws/v2');

const args = [
  'run',
  '-p',
  'trucoshi-realtime',
  '--features',
  'json-schema',
  '--bin',
  'ws_schema_v2',
  '--',
  outDir,
];

const res = spawnSync(cargo, args, { stdio: 'inherit' });
process.exit(res.status ?? 1);
