import React, { memo, useEffect, useRef } from 'react';
import AutoSizer from 'react-virtualized-auto-sizer';
import { FixedSizeGrid as Grid, GridChildComponentProps } from 'react-window';
import { Socket } from 'socket.io-client';

interface Emoji {
  emoji: string;
  count: number;
}

interface EmojiGridProps {
  topEmojis: Emoji[];
  socket: Socket;
  lang: string;
}

const MIN_COLUMN_WIDTH = 90;
const ROW_HEIGHT = 40;
const CELL_PADDING = 4;

const EmojiGrid: React.FC<EmojiGridProps> = ({ topEmojis, socket, lang }) => {
  return (
    <main id="emoji-grid" className="flex-grow w-full bg-white dark:bg-gray-900 overflow-hidden">
      <AutoSizer>
        {({ height, width }) => {
          const columnCount = Math.floor(width / MIN_COLUMN_WIDTH) || 1;
          const columnWidth = Math.floor(width / columnCount);
          const rowCount = Math.ceil(topEmojis.length / columnCount);

          return (
            <Grid
              columnCount={columnCount}
              columnWidth={columnWidth}
              height={height}
              rowCount={rowCount}
              rowHeight={ROW_HEIGHT}
              width={width}
              overscanRowCount={10}
              itemData={{ items: topEmojis, columnCount, socket, lang }}
            >
              {Cell}
            </Grid>
          );
        }}
      </AutoSizer>
    </main>
  );
};

const Cell = memo(({ columnIndex, rowIndex, style, data }: GridChildComponentProps) => {
  const { items, columnCount, socket, lang }: { items: Emoji[]; columnCount: number; socket: Socket; lang: string } =
    data;
  const index = rowIndex * columnCount + columnIndex;

  const { emoji, count } = items[index];

  const elRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    // don't flash if this is the first render
    if (isFirstRun.current) {
      return;
    }

    const el = elRef.current;
    if (!el) {
      return;
    }

    const isDarkMode = window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches;

    const colors = cellColors[isDarkMode ? 'dark' : 'light'];
    const animation = el.animate([{ backgroundColor: colors.highlight }, { backgroundColor: colors.default }], {
      duration: 500,
      iterations: 1,
    });

    return () => {
      animation.cancel();
    };
  }, [count]);

  // NOTE: order matters here, this needs to be set *after* the above reads it
  const isFirstRun = useRef<boolean>(true);
  useEffect(() => {
    isFirstRun.current = false;
    return () => {
      isFirstRun.current = true;
    };
  }, []);

  if (index >= items.length) {
    return null;
  }

  const cellStyle = {
    ...style,
    left: (style.left as number) + CELL_PADDING,
    top: (style.top as number) + CELL_PADDING,
    width: (style.width as number) - CELL_PADDING * 2,
    height: (style.height as number) - CELL_PADDING * 2,
  };

  const handleClick = () => {
    console.log(`Getting emoji info for ${emoji}`);
    socket.emit('getEmojiInfo', emoji);
    const url =
      lang === 'all' || lang === 'unknown' ?
        `https://bsky.app/search?q=${encodeURIComponent(emoji)}`
      : `https://bsky.app/search?q=${encodeURIComponent('lang:' + lang + ' ' + emoji)}`;
    window.open(url, '_blank');
  };

  return (
    <div
      ref={elRef}
      style={cellStyle}
      className="flex items-center justify-between bg-gray-50 dark:bg-gray-800 p-2 rounded shadow-sm cursor-pointer hover:bg-gray-100 dark:hover:bg-gray-600"
      onClick={handleClick}
    >
      <span className="text text-black dark:text-gray-100">{emoji}</span>
      <span className="text-xs text-gray-600 dark:text-gray-100">{count}</span>
    </div>
  );
});

const cellColors = {
  light: {
    highlight: '#fbe8ae',
    default: 'rgb(249 250 251)', // gray-50
  },
  dark: {
    highlight: '#666',
    default: 'rgb(45 55 72)', // gray-800
  },
};

export default memo(EmojiGrid);
