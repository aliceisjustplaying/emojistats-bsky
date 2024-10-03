import { useEffect, useRef, useState } from 'react';
import { Socket, io } from 'socket.io-client';

import EmojiGrid from './components/EmojiGrid.js';
import Footer from './components/Footer.js';
import Header from './components/Header.js';
import LanguageTabs from './components/LanguageTabs.js';

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

interface LanguageStat {
  language: string;
  count: number;
}

function App() {
  const [emojiStats, setEmojiStats] = useState<EmojiStats | null>(null);
  const [totalEmojiCount, setTotalEmojiCount] = useState<number>(0);
  const [languageStats, setLanguageStats] = useState<LanguageStat[]>([]);
  const [selectedLanguage, setSelectedLanguage] = useState<string>('all');
  const [currentEmojis, setCurrentEmojis] = useState<Array<{ emoji: string; count: number }>>([]);
  const socketRef = useRef<Socket | null>(null);

  useEffect(() => {
    const socket: Socket = io(import.meta.env.VITE_SOCKET_URL);

    // Handle incoming emoji stats
    socket.on('emojiStats', (data: EmojiStats) => {
      setEmojiStats(data);
      setTotalEmojiCount(data.processedEmojis);
      if (selectedLanguage === 'all') {
        setCurrentEmojis(data.topEmojis);
      }
    });

    // Handle incoming language stats
    socket.on('languageStats', (data: LanguageStat[]) => {
      setLanguageStats(data);
    });

    // Handle incoming top emojis for a specific language
    socket.on(
      'topEmojisForLanguage',
      (data: { language: string; topEmojis: Array<{ emoji: string; count: number }> }) => {
        if (data.language === selectedLanguage) {
          setCurrentEmojis(data.topEmojis);
        }
      },
    );

    socket.on('emojiInfo', (data) => {
      console.log('Emoji Info:', data);
    });

    socketRef.current = socket;

    // Clean up on unmount
    return () => {
      socket.disconnect();
    };
  }, [selectedLanguage]); // Include selectedLanguage to handle updates

  useEffect(() => {
    if (selectedLanguage !== 'all' && socketRef.current) {
      socketRef.current.emit('getTopEmojisForLanguage', selectedLanguage);
    } else if (emojiStats) {
      setCurrentEmojis(emojiStats.topEmojis);
    }
  }, [selectedLanguage, emojiStats]);

  const handleLanguageSelect = (language: string) => {
    setSelectedLanguage(language);
  };

  return (
    <div className="flex flex-col h-screen">
      <Header />
      <LanguageTabs
        languages={languageStats}
        selectedLanguage={selectedLanguage}
        onSelect={handleLanguageSelect}
        totalEmojiCount={totalEmojiCount}
      />
      <EmojiGrid topEmojis={currentEmojis} socket={socketRef.current!} lang={selectedLanguage} />
      <Footer
        stats={
          emojiStats || {
            processedPosts: 0,
            processedEmojis: 0,
            postsWithEmojis: 0,
            postsWithoutEmojis: 0,
            ratio: 'N/A',
            topEmojis: [],
          }
        }
      />
    </div>
  );
}

export default App;
