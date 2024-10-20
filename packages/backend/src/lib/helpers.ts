export const emojiToCodePoint = (emoji: string) =>
  [...emoji].map((char) => char.codePointAt(0)?.toString(16).padStart(4, '0')).join(' ');

export function codePointToEmoji(codePoint: string): string {
  const codePoints = codePoint.split(codePoint.includes(' ') ? ' ' : '-').map((cp) => parseInt(cp, 16));
  return String.fromCodePoint(...codePoints);
}
