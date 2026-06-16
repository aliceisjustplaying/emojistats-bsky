import { isIP } from 'node:net';

export type UnusablePdsHostKind =
  | 'invalid'
  | 'loopback'
  | 'private'
  | 'link-local'
  | 'reserved';

export type PublicPdsHost = string & {
  readonly __publicPdsHost: unique symbol;
};

export type PublicPdsHostAdmission =
  | { ok: true; host: PublicPdsHost }
  | { ok: false; host: string; kind: UnusablePdsHostKind };

function parseHostname(host: string): string | null {
  const ipVersion = isIP(host);
  if (ipVersion === 4 || ipVersion === 6) return host.toLowerCase();
  try {
    const url = host.includes('://')
      ? new URL(host)
      : new URL(`https://${host}`);
    const hostname = url.hostname.toLowerCase();
    if (hostname === '') return null;
    return hostname.startsWith('[') && hostname.endsWith(']')
      ? hostname.slice(1, -1)
      : hostname;
  } catch {
    return null;
  }
}

function parseIpv4Octets(host: string): number[] | null {
  const parts = host.split('.');
  if (parts.length !== 4) return null;
  const octets = parts.map((part) => {
    if (!/^\d+$/.test(part)) return Number.NaN;
    return Number(part);
  });
  return octets.every(
    (octet) => Number.isInteger(octet) && octet >= 0 && octet <= 255,
  )
    ? octets
    : null;
}

function ipv4FromMappedIpv6(host: string): string | null {
  const compact = host.match(/^::ffff:(.+)$/i)?.[1];
  const expanded = host.match(/^0:0:0:0:0:ffff:(.+)$/i)?.[1];
  const suffix = compact ?? expanded;
  if (suffix === undefined) return null;
  if (suffix.includes('.')) return suffix;
  const parts = suffix.split(':');
  if (parts.length !== 2) return null;
  const [highRaw, lowRaw] = parts;
  const high = Number.parseInt(highRaw ?? '', 16);
  const low = Number.parseInt(lowRaw ?? '', 16);
  if (
    !Number.isInteger(high) ||
    !Number.isInteger(low) ||
    high < 0 ||
    high > 0xffff ||
    low < 0 ||
    low > 0xffff
  )
    return null;
  return [(high >> 8) & 0xff, high & 0xff, (low >> 8) & 0xff, low & 0xff].join(
    '.',
  );
}

function classifyIpv4(host: string): UnusablePdsHostKind | null {
  const octets = parseIpv4Octets(host);
  if (octets === null) return null;
  const [a, b, c, d] = octets;
  if (a === 127 || (a === 0 && b === 0 && c === 0 && d === 0))
    return 'loopback';
  if (a === 10) return 'private';
  if (a === 172 && b >= 16 && b <= 31) return 'private';
  if (a === 192 && b === 168) return 'private';
  if (a === 169 && b === 254) return 'link-local';
  if (a === 100 && b >= 64 && b <= 127) return 'reserved';
  if (a === 192 && b === 0 && c === 2) return 'reserved';
  if (a === 198 && b === 18) return 'reserved';
  if (a === 198 && b === 19) return 'reserved';
  if (a === 198 && b === 51 && c === 100) return 'reserved';
  if (a === 203 && b === 0 && c === 113) return 'reserved';
  if (a >= 224) return 'reserved';
  return null;
}

function classifyIpv6(host: string): UnusablePdsHostKind | null {
  if (isIP(host) !== 6) return null;
  const mappedIpv4 = ipv4FromMappedIpv6(host);
  if (mappedIpv4 !== null) return classifyIpv4(mappedIpv4);
  if (host === '::1' || host === '0:0:0:0:0:0:0:1') return 'loopback';
  if (host === '::' || host === '0:0:0:0:0:0:0:0') return 'reserved';
  if (/^f[cd][0-9a-f]{2}:/i.test(host)) return 'private';
  if (/^fe[89ab][0-9a-f]:/i.test(host)) return 'link-local';
  if (/^2001:db8:/i.test(host)) return 'reserved';
  return null;
}

function hasReservedSuffix(host: string): boolean {
  return (
    host.endsWith('.test') ||
    host.endsWith('.invalid') ||
    host.endsWith('.example') ||
    host.endsWith('.local') ||
    host.endsWith('.localhost') ||
    host.endsWith('.internal') ||
    host.endsWith('.home.arpa')
  );
}

export function classifyUnusablePdsHost(
  host: string,
): UnusablePdsHostKind | null {
  const hostname = parseHostname(host);
  if (hostname === null) return 'invalid';
  if (hostname === 'localhost') return 'loopback';
  if (hasReservedSuffix(hostname)) {
    return hostname.endsWith('.localhost') ? 'loopback' : 'reserved';
  }
  return classifyIpv4(hostname) ?? classifyIpv6(hostname);
}

export function validatePublicPdsHost(host: string): PublicPdsHostAdmission {
  const kind = classifyUnusablePdsHost(host);
  if (kind !== null) return { ok: false, host, kind };
  return { ok: true, host: host as PublicPdsHost };
}

export function unusablePdsHostMessage(
  host: string,
  kind: UnusablePdsHostKind,
): string {
  return `non-public PDS address: ${kind}`;
}
