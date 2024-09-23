import { useEffect, useState } from 'react';
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

  if (!emojiStats) {
    return (
      <div className="flex items-center justify-center h-screen bg-gray-900 text-white">
        Loading...
      </div>
    );
  }

  return (
    <div className="flex flex-col h-screen bg-gray-900 text-white">
      <Header />
      <EmojiGrid topEmojis={emojiStats.topEmojis} />
      <Footer stats={emojiStats} />
    </div>
  );
}

export default App;
