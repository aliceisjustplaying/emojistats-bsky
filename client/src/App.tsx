import { useEffect, useState } from 'react';
import { io, Socket } from 'socket.io-client';

interface EmojiStats {
  processedPosts: number;
  processedEmojis: number;
  postsWithEmojis: number;
  postsWithoutEmojis: number;
  ratio: string;
  top10Emojis: Array<{
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
      <h1>Emoji Tracker</h1>
      <p>Processed Posts: {emojiStats.processedPosts}</p>
      <p>Processed Emojis: {emojiStats.processedEmojis}</p>
      <p>Posts with Emojis: {emojiStats.postsWithEmojis}</p>
      <p>Posts without Emojis: {emojiStats.postsWithoutEmojis}</p>
      <p>Ratio: {emojiStats.ratio}</p>
      <h2>Top 10 Emojis</h2>
      <ul>
        {emojiStats.top10Emojis.map(({ emoji, count }) => (
          <li key={emoji}>{`${emoji}: ${count}`}</li>
        ))}
      </ul>
    </div>
  );
}

export default App;
