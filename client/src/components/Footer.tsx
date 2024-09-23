import React from 'react';

interface EmojiStats {
  processedPosts: number;
  processedEmojis: number;
  postsWithEmojis: number;
  postsWithoutEmojis: number;
  ratio: string;
}

interface FooterProps {
  stats: EmojiStats;
}

function Footer({ stats }: FooterProps) {
  return (
    <footer className="w-full bg-blue-600 p-4 h-16 flex-none">
      <div className="flex flex-col sm:flex-row justify-between items-center text-white space-y-2 sm:space-y-0">
        <span>Processed Posts: {stats.processedPosts}</span>
        <span>Processed Emojis: {stats.processedEmojis}</span>
        <span>Posts with Emojis: {stats.postsWithEmojis}</span>
        <span>Posts without Emojis: {stats.postsWithoutEmojis}</span>
        <span>Ratio: {stats.ratio}</span>
      </div>
    </footer>
  );
}

export default Footer;
