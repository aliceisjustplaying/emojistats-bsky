import React, { memo } from 'react';
import { FixedSizeGrid as Grid, GridChildComponentProps } from 'react-window';
import AutoSizer from 'react-virtualized-auto-sizer';

interface Emoji {
  emoji: string;
  count: number;
}

interface EmojiGridProps {
  topEmojis: Emoji[];
}

const MIN_COLUMN_WIDTH = 80;
const ROW_HEIGHT = 40;
const CELL_PADDING = 4;

const EmojiGrid: React.FC<EmojiGridProps> = ({ topEmojis }) => {
  const Cell = memo(({ columnIndex, rowIndex, style, data }: GridChildComponentProps) => {
    const { items, columnCount } = data;
    const index = rowIndex * columnCount + columnIndex;
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

    return (
      <div style={cellStyle} className="flex items-center justify-between bg-gray-50 p-2 rounded shadow-sm">
        <span className="text-lg text-black">{emoji}</span>
        <span className="text-sm text-gray-600">{count}</span>
      </div>
    );
  });

  return (
    <main className="flex-grow w-full p-4 bg-white overflow-hidden">
      <AutoSizer>
        {({ height, width }) => {
          const columnCount = Math.floor(width / MIN_COLUMN_WIDTH);
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
              itemData={{ items: topEmojis, columnCount }}
            >
              {Cell}
            </Grid>
          );
        }}
      </AutoSizer>
    </main>
  );
};

export default memo(EmojiGrid);
