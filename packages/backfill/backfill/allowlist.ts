import { readFile } from "node:fs/promises";

export async function loadAllowlist(
  path?: string,
): Promise<Set<string> | null> {
  if (!path) return null;
  const data = await readFile(path, "utf8");
  const entries = data
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter((line) => line.length > 0 && !line.startsWith("#"));
  return new Set(entries);
}
