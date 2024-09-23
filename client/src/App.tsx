import { useEffect, useState, useCallback } from 'react';
import { io, Socket } from 'socket.io-client';
import Header from './components/Header';
import EmojiGrid from './components/EmojiGrid';
import Footer from './components/Footer';

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

  useEffect(() => {
    const socket: Socket = io('http://localhost:3000');

    const handleEmojiStats = (data: EmojiStats) => {
      setEmojiStats(data);
    };

    socket.on('emojiStats', handleEmojiStats);

    return () => {
      socket.off('emojiStats', handleEmojiStats);
      socket.disconnect();
    };
  }, []);

  if (!emojiStats) {
    return (
      <div className="flex items-center justify-center h-screen bg-gray-900 text-white">
        Loading...
      </div>
    );
  }

  return (
    <div className="flex flex-col h-screen text-white">
      <Header />
      <EmojiGrid topEmojis={emojiStats.topEmojis} />
      <Footer stats={emojiStats} />
    </div>
  );
}

export default App;
