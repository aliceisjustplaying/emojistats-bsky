import fs from 'node:fs';
import path from 'node:path';

import logger from './logger.js';

export interface CursorLoadResult {
  cursorUs: number;
  fromOverride: boolean;
}

export class CursorStore {
  constructor(
    private readonly filePath: string,
    private readonly overridePath: string,
  ) {}

  /**
   * The override file is intentionally left in place after a successful read
   * (matches the backend) — remove it by hand once the rewind has taken.
   */
  load(): CursorLoadResult | undefined {
    const override = this.read(this.overridePath);
    if (override !== undefined)
      return { cursorUs: override, fromOverride: true };
    const saved = this.read(this.filePath);
    if (saved !== undefined) return { cursorUs: saved, fromOverride: false };
    return undefined;
  }

  save(cursorUs: number): void {
    fs.mkdirSync(path.dirname(this.filePath), { recursive: true });
    // Write-then-rename so a crash mid-write can never leave a truncated cursor.
    const tmpPath = `${this.filePath}.tmp`;
    fs.writeFileSync(tmpPath, String(cursorUs));
    fs.renameSync(tmpPath, this.filePath);
  }

  private read(file: string): number | undefined {
    let raw: string;
    try {
      raw = fs.readFileSync(file, 'utf8');
    } catch {
      return undefined; // missing file is the normal first-run case
    }
    const value = Number(raw.trim());
    if (!Number.isSafeInteger(value) || value <= 0) {
      logger.warn(
        { file, contents: raw.trim() },
        'Ignoring cursor file: not a positive integer',
      );
      return undefined;
    }
    return value;
  }
}
