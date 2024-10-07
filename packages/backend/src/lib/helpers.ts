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
