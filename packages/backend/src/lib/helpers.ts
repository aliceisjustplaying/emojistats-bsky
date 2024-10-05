export function emojiToCodePoint(emoji: string): string {
  return [...emoji].map((char) => char.codePointAt(0)?.toString(16).padStart(4, '0')).join(' ');
}

export function codePointToEmoji(codePoint: string): string {
  const codePoints =
    codePoint.includes(' ') ?
      codePoint.split(' ').map((cp) => parseInt(cp, 16))
    : codePoint.split('-').map((cp) => parseInt(cp, 16));
  return String.fromCodePoint(...codePoints);
}

type AnyObject = Record<string, unknown>;

export function lowercaseObject<T>(input: T): T {
  if (Array.isArray(input)) {
    // eslint-disable-next-line @typescript-eslint/no-unsafe-return
    return input.map((item) => lowercaseObject(item)) as unknown as T;
  } else if (input !== null && typeof input === 'object') {
    // Preserve Date objects and other non-plain objects
    if (input instanceof Date) {
      return input;
    }

    const lowercasedObj: AnyObject = {};

    for (const key in input) {
      if (Object.prototype.hasOwnProperty.call(input, key)) {
        const lowerKey = typeof key === 'string' ? key.toLowerCase() : key;
        const value = (input as AnyObject)[key];

        if (typeof value === 'string') {
          lowercasedObj[lowerKey] = value.toLowerCase();
        } else {
          lowercasedObj[lowerKey] = lowercaseObject(value);
        }
      }
    }

    return lowercasedObj as T;
  } else if (typeof input === 'string') {
    return input.toLowerCase() as unknown as T;
  }

  // For other types (number, boolean, etc.), return as-is
  return input;
}
