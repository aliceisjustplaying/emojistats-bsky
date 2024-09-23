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
    <main className="flex-grow p-4 bg-white overflow-auto">
      <h2 className="text-xl font-semibold mb-4 text-gray-800">Top 100 Emojis</h2>
      <div className="grid grid-cols-2 sm:grid-cols-4 md:grid-cols-6 lg:grid-cols-10 gap-2">
        {topEmojis.map(({ emoji, count }) => (
          <div
            key={emoji}
            className="flex items-center justify-center space-x-2 bg-gray-50 p-1 rounded shadow-sm"
          >
            <span className="text-2xl text-black">{emoji}</span>
            <span className="text-gray-900 text-sm">{count}</span>
          </div>
        ))}
      </div>
    </main>
  );
}

export default EmojiGrid;
