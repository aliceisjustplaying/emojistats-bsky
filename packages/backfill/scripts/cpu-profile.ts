// Sampling CPU profiler for a live crawler: kill -USR1 <node pid> to open the
// inspector, then: bun scripts/cpu-profile.ts 127.0.0.1:9229
// Prints top stacks by self-time. Cracked bottlenecks #4, #11 and the park
// race in minutes each — profile before fixing.

import process from 'node:process';

interface CallFrame {
  functionName: string;
  url: string;
  lineNumber: number;
}
interface ProfileNode {
  id: number;
  callFrame: CallFrame;
  children?: number[];
}
interface Profile {
  nodes: ProfileNode[];
  samples: number[];
  startTime: number;
  endTime: number;
}

const target = process.argv[2];
if (target === undefined)
  throw new Error('usage: bun scripts/cpu-profile.ts <host:inspector-port>');
const list = (await fetch(`http://${target}/json/list`).then((r) =>
  r.json(),
)) as Array<{ webSocketDebuggerUrl: string }>;
const ws = new WebSocket(list[0].webSocketDebuggerUrl);
let id = 0;
const pending = new Map<number, (result: unknown) => void>();
const send = (method: string, params = {}): Promise<unknown> =>
  new Promise((resolve) => {
    id += 1;
    pending.set(id, resolve);
    ws.send(JSON.stringify({ id, method, params }));
  });
ws.addEventListener('message', (ev) => {
  const m = JSON.parse(ev.data as string) as { id?: number; result?: unknown };
  if (m.id !== undefined && pending.has(m.id)) {
    pending.get(m.id)!(m.result);
    pending.delete(m.id);
  }
});
await new Promise<void>((resolve) => {
  ws.addEventListener('open', () => {
    resolve();
  });
});
await send('Profiler.enable');
await send('Profiler.start');
await new Promise((resolve) => {
  setTimeout(resolve, 10_000);
});
const { profile } = (await send('Profiler.stop')) as { profile: Profile };

const nodes = new Map(profile.nodes.map((n) => [n.id, n]));
const parentOf = new Map<number, number>();
for (const n of profile.nodes)
  for (const c of n.children ?? []) parentOf.set(c, n.id);
const stackCount = new Map<string, number>();
for (const s of profile.samples) {
  const frames: string[] = [];
  let cur: number | undefined = s;
  let depth = 0;
  while (cur !== undefined && depth < 12) {
    const n = nodes.get(cur);
    if (!n) break;
    const f = n.callFrame;
    frames.push(
      `${f.functionName || '(anon)'}@${f.url.split('/').at(-1) ?? ''}:${f.lineNumber}`,
    );
    cur = parentOf.get(cur);
    depth += 1;
  }
  const key = frames.slice(0, 6).join(' < ');
  stackCount.set(key, (stackCount.get(key) ?? 0) + 1);
}
const total = profile.samples.length;
const top = [...stackCount.entries()]
  .toSorted((a, b) => b[1] - a[1])
  .slice(0, 12);
for (const [k, v] of top)
  console.log(`${((v / total) * 100).toFixed(1)}%  ${k}`);
ws.close();
