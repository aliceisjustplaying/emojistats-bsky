import React, { memo, useEffect, useRef, useState } from 'react';
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
    <main className="flex-grow w-full px-1 bg-white overflow-hidden">
      <AutoSizer>
        {({ height, width }) => {
          const columnCount = Math.floor(width / MIN_COLUMN_WIDTH) || 1;
          const columnWidth = width / columnCount;
          const rowCount = Math.ceil(topEmojis.length / columnCount);

          return (
            <Grid
              columnCount={columnCount}
              columnWidth={columnWidth}
              height={height}
              rowCount={rowCount}
              rowHeight={ROW_HEIGHT}
              width={width}
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

  const cellStyle = {
    ...style,
    left: (style.left as number) + CELL_PADDING,
    top: (style.top as number) + CELL_PADDING,
    width: (style.width as number) - CELL_PADDING * 2,
    height: (style.height as number) - CELL_PADDING * 2,
  };

  const [isBlinking, setIsBlinking] = useState(false);
  const [prevCount, setPrevCount] = useState(count);

  useEffect(() => {
    if (prevCount !== count) {
      setIsBlinking(true);
      console.log('Blinking');
      setPrevCount(count);

      // Remove the blink effect after the animation duration
      const timer = setTimeout(() => {
        setIsBlinking(false);
      }, 500); // Duration should match the CSS animation duration

      return () => clearTimeout(timer);
    }
  }, [count, prevCount]);

  if (index >= items.length) {
    return null;
  }

  const handleClick = () => {
    console.log(`Getting emoji info for ${emoji}`);
    socket.emit('getEmojiInfo', emoji);
    const url =
      lang === 'all' || lang === 'unknown' ?
        `https://bsky.app/search?q=${encodeURIComponent(emoji)}`
      : `https://bsky.app/search?q=${encodeURIComponent('lang:' + lang + ' ' + emoji)}`;
    // console.log(url);
    window.open(url, '_blank');
  };

  return (
    <div
      style={cellStyle}
      className={`flex items-center justify-between bg-gray-50 p-2 rounded shadow-sm cursor-pointer hover:bg-gray-100 ${isBlinking ? 'blink' : ''}`}
      onClick={handleClick}
    >
      <span className="text text-black">{emoji}</span>
      <span className="text-xs text-gray-600">{count}</span>
    </div>
  );
});

export default memo(EmojiGrid);
