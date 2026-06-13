import { version } from '../constants';

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
  const ratio = Number(stats.ratio);
  const ratioLabel = Number.isFinite(ratio)
    ? `${(ratio * 100).toFixed(2)}%`
    : 'N/A';

  return (
    <footer className="w-full bg-gray-200 dark:bg-gray-900 p-1 flex flex-row justify-end items-center">
      <span className="w-full sm:w-auto text-xs md:text-sm text-center px-1 sm:px-2 text-gray-900 dark:text-gray-100">
        Posts: {stats.processedPosts}
      </span>
      <span className="w-full sm:w-auto text-xs md:text-sm text-center px-1 sm:px-2 text-gray-900 dark:text-gray-100">
        Emoji posts: {stats.postsWithEmojis}
      </span>
      <span className="w-full sm:w-auto text-xs md:text-sm text-center px-1 sm:px-2 text-gray-900 dark:text-gray-100">
        Ratio: {ratioLabel}
      </span>
      <span className="w-full sm:w-auto text-xs md:text-sm text-center px-1 sm:px-2 text-gray-900 dark:text-gray-100">
        by{' '}
        <a href="https://alice.bsky.sh/" target="_blank" rel="noreferrer">
          Alice
        </a>
      </span>
      <span className="hidden md:inline text-xs md:text-sm text-center px-1 sm:px-2 text-gray-900 dark:text-gray-100">
        Version {version}
      </span>

      <span className="w-full sm:w-auto text-xs md:text-sm text-center flex flex-row items-center justify-center px-1 sm:px-2 text-gray-900 dark:text-gray-100">
        <span className="inline md:hidden">v{version}&nbsp;&nbsp;</span>
        <a
          href="https://github.com/aliceisjustplaying/emojistats-bsky"
          target="_blank"
          rel="noreferrer"
          className="leading-none"
        >
          <img
            src="/gh.png"
            alt="GitHub"
            className="w-4 h-4 mb-0 inline-block dark:hidden"
          />
          <img
            src="/gh-white.png"
            alt="GitHub"
            className="w-4 h-4 mb-0 hidden dark:inline-block"
          />
        </a>
      </span>
    </footer>
  );
}

export default Footer;
