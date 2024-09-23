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
      <div className="grid grid-cols-2 sm:grid-cols-4 md:grid-cols-6 lg:grid-cols-10 gap-2">
        {topEmojis.map(({ emoji, count }) => (
          <div
            key={emoji}
            className="flex items-center justify-start space-x-2 bg-gray-50 p-1 rounded shadow-sm"
          >
            <span className="text-xl text-black">{emoji}</span> {/* Reduced font size */}
            <span className="text-gray-900 text-sm">{count}</span> {/* Reduced font size */}
          </div>
        ))}
      </div>
    </main>
  );
}

export default EmojiGrid;
