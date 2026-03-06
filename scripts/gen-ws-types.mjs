import { readFile, writeFile, mkdir } from 'node:fs/promises';
import { dirname } from 'node:path';
import { compile } from 'json-schema-to-typescript';

const CHECK = process.argv.includes('--check');

async function genOne({ inPath, outPath, rootName, banner }) {
  const raw = await readFile(inPath, 'utf8');
  const schema = JSON.parse(raw);

  // json-schema-to-typescript prefers `schema.title` when generating the root type name.
  // Override it so downstream consumers get stable, short names (WsInMessage/WsOutMessage/etc).
  schema.title = rootName;

  let ts = await compile(schema, rootName, {
    bannerComment: banner,
    style: {
      singleQuote: true,
      semi: true,
    },
  });

  // Cleanup: json-schema-to-typescript sometimes emits primitive aliases like:
  //   export type String = string;
  // which then propagates into real fields (`id?: String`).
  // For our protocol types, this is just noise; collapse to `string`.
  ts = ts.replace(/^export type String = string;\n/m, '');
  ts = ts.replace(/\bString\b/g, 'string');

  if (CHECK) {
    let onDisk = null;
    try {
      onDisk = await readFile(outPath, 'utf8');
    } catch {
      onDisk = null;
    }

    if (onDisk !== ts) {
      console.error(`${outPath} is out of date; run: npm run gen:ws:types`);
      process.exitCode = 1;
    }

    return;
  }

  await mkdir(dirname(outPath), { recursive: true });
  await writeFile(outPath, ts, 'utf8');
}

const banner = `/*
 * AUTO-GENERATED FILE. DO NOT EDIT.
 *
 * Generated from JSON Schema under schemas/ws/v2/*.json
 *
 * Regenerate with:
 *   npm run gen:ws:types
 */\n`;

await genOne({
  inPath: 'schemas/ws/v2/in.json',
  outPath: 'schemas/ws/v2/in.ts',
  rootName: 'WsInMessage',
  banner,
});

await genOne({
  inPath: 'schemas/ws/v2/out.json',
  outPath: 'schemas/ws/v2/out.ts',
  rootName: 'WsOutMessage',
  banner,
});

await genOne({
  inPath: 'schemas/ws/v2/c2s.json',
  outPath: 'schemas/ws/v2/c2s.ts',
  rootName: 'C2sMessage',
  banner,
});

await genOne({
  inPath: 'schemas/ws/v2/s2c.json',
  outPath: 'schemas/ws/v2/s2c.ts',
  rootName: 'S2cMessage',
  banner,
});

if (CHECK) {
  if (process.exitCode && process.exitCode !== 0) {
    process.exit(process.exitCode);
  }
  console.log('WS v2 TypeScript types are up to date.');
} else {
  console.log(
    'Generated schemas/ws/v2/in.ts, out.ts, c2s.ts, and s2c.ts (from JSON Schemas)'
  );
}
