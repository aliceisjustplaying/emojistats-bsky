import React from 'react';

interface Emoji {
  emoji: string;
  count: number;
}

interface EmojiGridProps {
  topEmojis: Emoji[];
}

function EmojiGrid({ topEmojis }: EmojiGridProps) {
  return (
    <main className="flex-grow p-2 bg-gray-800 overflow-auto">
      <h2 className="text-xl font-semibold mb-2 text-white">Top 100 Emojis</h2>
      <div className="grid grid-cols-2 sm:grid-cols-4 md:grid-cols-6 lg:grid-cols-10 gap-2">
        {topEmojis.map(({ emoji, count }) => (
          <div
            key={emoji}
            className="flex items-center justify-center space-x-1 bg-gray-700 p-2 rounded shadow-md"
          >
            <span className="text-xl">{emoji}</span>
            <span className="text-white text-sm">{count}</span>
          </div>
        ))}
      </div>
    </main>
  );
}

export default EmojiGrid;
