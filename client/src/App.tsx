import { useEffect, useState } from 'react';
import { io, Socket } from 'socket.io-client';

interface EmojiStats {
  processedPosts: number;
  processedEmojis: number;
  postsWithEmojis: number;
  postsWithoutEmojis: number;
  ratio: string;
  topEmojis: Array<{
    emoji: string;
    count: number;
  }>;
}

function App() {
  const [emojiStats, setEmojiStats] = useState<EmojiStats | null>(null);
  const [socket, setSocket] = useState<Socket | null>(null);

  useEffect(() => {
    const newSocket = io('http://localhost:3000'); // Connects to the host that serves the page
    setSocket(newSocket);

    newSocket.on('emojiStats', (data: EmojiStats) => {
      setEmojiStats(data);
    });

    return () => {
      newSocket.disconnect();
    };
  }, []);

  if (!emojiStats) return <div>Loading...</div>;

  return (
    <div>
      <h1 className="text-3xl font-bold underline">
        Emoji Tracker for Bluesky 🦋
      </h1>
      <p>Processed Posts: {emojiStats.processedPosts}</p>
      <p>Processed Emojis: {emojiStats.processedEmojis}</p>
      <p>Posts with Emojis: {emojiStats.postsWithEmojis}</p>
      <p>Posts without Emojis: {emojiStats.postsWithoutEmojis}</p>
      <p>Ratio: {emojiStats.ratio}</p>
      <h2>Top 100 Emojis</h2>
      <div style={{
        display: 'grid',
        gridTemplateColumns: 'repeat(10, 1fr)',
        gap: '10px',
        maxWidth: '800px',
        margin: '0 auto'
      }}>
        {emojiStats.topEmojis.map(({ emoji, count }) => (
          <div key={emoji} style={{ textAlign: 'center' }}>
            <div style={{ fontSize: '24px' }}>{emoji}</div>
            <div>{count}</div>
          </div>
        ))}
      </div>
    </div>
  );
}

export default App;
