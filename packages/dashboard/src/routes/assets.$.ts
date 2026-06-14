import { access, readFile } from 'node:fs/promises';
import { resolve, sep } from 'node:path';

import { createFileRoute } from '@tanstack/react-router';

const ASSET_ROOT = resolve(process.cwd(), 'dist/client/assets');

const CONTENT_TYPES: Record<string, string> = {
  '.css': 'text/css; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.map': 'application/json; charset=utf-8',
  '.svg': 'image/svg+xml',
  '.png': 'image/png',
  '.jpg': 'image/jpeg',
  '.jpeg': 'image/jpeg',
  '.webp': 'image/webp',
  '.ico': 'image/x-icon',
};

function contentType(path: string): string {
  const ext = path.slice(path.lastIndexOf('.')).toLowerCase();
  return CONTENT_TYPES[ext] ?? 'application/octet-stream';
}

function assetPath(splat: string): string | null {
  const fullPath = resolve(ASSET_ROOT, splat);
  if (fullPath === ASSET_ROOT || !fullPath.startsWith(`${ASSET_ROOT}${sep}`)) {
    return null;
  }
  return fullPath;
}

export const Route = createFileRoute('/assets/$')({
  server: {
    handlers: {
      GET: async ({ params }) => {
        const fullPath = assetPath(params._splat ?? '');
        if (fullPath === null)
          return new Response('Not Found', { status: 404 });

        try {
          await access(fullPath);
          const body = await readFile(fullPath);
          return new Response(body, {
            headers: {
              'Cache-Control': 'public, max-age=31536000, immutable',
              'Content-Type': contentType(fullPath),
            },
          });
        } catch {
          return new Response('Not Found', { status: 404 });
        }
      },
    },
  },
});
