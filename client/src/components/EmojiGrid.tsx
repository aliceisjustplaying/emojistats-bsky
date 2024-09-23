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

const COLUMN_COUNT = 15; // Number of columns matching your grid setup
const ROW_HEIGHT = 30; // Increased to accommodate padding
const COLUMN_WIDTH = 98; // Increased to accommodate padding
const CELL_PADDING = 2; // New constant for cell padding

const EmojiGrid: React.FC<EmojiGridProps> = ({ topEmojis }) => {
  const rowCount = Math.ceil(topEmojis.length / COLUMN_COUNT);

  const Cell = memo(({ columnIndex, rowIndex, style }: GridChildComponentProps) => {
    const index = rowIndex * COLUMN_COUNT + columnIndex;
    if (index >= topEmojis.length) {
      return null;
    }
    const { emoji, count } = topEmojis[index];

    const cellStyle = {
      ...style,
      left: (style.left as number) + CELL_PADDING,
      top: (style.top as number) + CELL_PADDING,
      width: (style.width as number) - CELL_PADDING * 2,
      height: (style.height as number) - CELL_PADDING * 2,
    };

    return (
      <div style={cellStyle} className="flex items-center justify-start space-x-2 bg-gray-50 p-1 rounded shadow-sm">
        <span className="text-lg text-black">{emoji}</span>
        <span className="text-gray-900">{count}</span>
      </div>
    );
  });

  return (
    <main className="flex-grow w-full p-4 bg-white overflow-auto">
      <AutoSizer>
        {({ height, width }) => (
          <Grid
  columnCount={COLUMN_COUNT}
  columnWidth={COLUMN_WIDTH}
  height={height}
  rowCount={rowCount}
  rowHeight={ROW_HEIGHT}
  width={width}
  overscanRowCount={5}
>
  {Cell}
</Grid>
        )}
      </AutoSizer>
    </main>
  );
};

export default memo(EmojiGrid);
