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

interface CellProps extends GridChildComponentProps {
  data: {
    items: Emoji[];
    columnCount: number;
    socket: Socket;
    lang: string;
  };
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

const Cell: React.FC<CellProps> = memo(({ columnIndex, rowIndex, style, data }) => {
  const { items, columnCount, socket, lang } = data;
  const index = rowIndex * columnCount + columnIndex;

  // Early return if index is out of bounds
  if (index >= items.length) {
    return null;
  }

  const { emoji, count } = items[index];

  const cellStyle = {
    ...style,
    left: (style.left as number) + CELL_PADDING,
    top: (style.top as number) + CELL_PADDING,
    width: (style.width as number) - CELL_PADDING * 2,
    height: (style.height as number) - CELL_PADDING * 2,
  };

  // Ref to store the previous count
  const prevCountRef = useRef<number>(count);

  // State to control the blink effect
  const [isBlinking, setIsBlinking] = useState(false);

  // Effect to detect count changes
  useEffect(() => {
    if (prevCountRef.current !== count) {
      setIsBlinking(true);
      console.log('Blinking');

      // Update the previous count
      prevCountRef.current = count;

      // Remove the blink effect after the animation duration
      const timer = setTimeout(() => {
        setIsBlinking(false);
      }, 500); // Duration should match the CSS animation duration

      return () => clearTimeout(timer);
    }
  }, [count]);

  const handleClick = () => {
    console.log(`Getting emoji info for ${emoji}`);
    socket.emit('getEmojiInfo', emoji);
    const query =
      lang === 'all' || lang === 'unknown' ? encodeURIComponent(emoji) : encodeURIComponent(`lang:${lang} ${emoji}`);
    const url = `https://bsky.app/search?q=${query}`;
    window.open(url, '_blank');
  };

  return (
    <div
      style={cellStyle}
      className={`flex items-center justify-between bg-gray-50 p-2 rounded shadow-sm cursor-pointer hover:bg-gray-100 ${
        isBlinking ? 'blink' : ''
      }`}
      onClick={handleClick}
    >
      <span className="text text-black">{emoji}</span>
      <span className="text-xs text-gray-600">{count}</span>
    </div>
  );
}, areEqual);

// Custom comparison function for React.memo
function areEqual(prevProps: CellProps, nextProps: CellProps) {
  const prevIndex = prevProps.rowIndex * prevProps.data.columnCount + prevProps.columnIndex;
  const nextIndex = nextProps.rowIndex * nextProps.data.columnCount + nextProps.columnIndex;

  const prevItem = prevProps.data.items[prevIndex];
  const nextItem = nextProps.data.items[nextIndex];

  // Compare emoji and count
  return prevItem?.emoji === nextItem?.emoji && prevItem?.count === nextItem?.count;
}

export default memo(EmojiGrid);
