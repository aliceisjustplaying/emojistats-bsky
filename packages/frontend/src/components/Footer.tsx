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
    <footer className="w-full bg-gray-200 p-1 flex flex-row justify-end items-center">
      <span className="w-full sm:w-auto text-xs md:text-sm text-center px-2">Posts: {stats.processedPosts}</span>
      <span className="w-full sm:w-auto text-xs md:text-sm text-center px-2">Emojis: {stats.processedEmojis}</span>
      {/* <span className="w-full sm:w-auto text-center">Posts with Emojis: {stats.postsWithEmojis}</span>
        <span className="w-full sm:w-auto text-center">Posts without Emojis: {stats.postsWithoutEmojis}</span> */}
      <span className="w-full sm:w-auto text-xs md:text-sm text-center px-2">
        Ratio: {(Number(stats.ratio) * 100).toFixed(2)}%
      </span>
      <span className="w-full sm:w-auto text-xs md:text-sm text-center px-2">
        Created by <a href="https://alice.bsky.sh/">Alice</a>
      </span>
    </footer>
  );
}

export default Footer;
