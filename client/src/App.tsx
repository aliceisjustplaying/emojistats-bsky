import { useEffect, useState, useRef } from 'react';
import { io, Socket } from 'socket.io-client';
import Header from './components/Header';
import EmojiGrid from './components/EmojiGrid';
import Footer from './components/Footer';
import LanguageTabs from './components/LanguageTabs';
import * as Tabs from '@radix-ui/react-tabs';


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
  const [languageStats, setLanguageStats] = useState<LanguageStat[]>([]);
  const [selectedLanguage, setSelectedLanguage] = useState<string>('all');
  const [currentEmojis, setCurrentEmojis] = useState<Array<{ emoji: string; count: number }>>([]);
  const socketRef = useRef<Socket | null>(null);
  const selectedLanguageRef = useRef<string>(selectedLanguage);
  const [loading, setLoading] = useState<boolean>(false);

  // Update the ref whenever selectedLanguage changes
  useEffect(() => {
    selectedLanguageRef.current = selectedLanguage;
  }, [selectedLanguage]);

  useEffect(() => {
    // Initialize socket connection once
    const socket: Socket = io('http://localhost:3000');

    // Handle incoming emoji stats
    socket.on('emojiStats', (data: EmojiStats) => {
      setEmojiStats(data);
      if (selectedLanguageRef.current === 'all') {
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
        if (data.language === selectedLanguageRef.current) {
          setCurrentEmojis(data.topEmojis);
          setLoading(false);
        }
      }
    );

    socketRef.current = socket;

    // Clean up on unmount
    return () => {
      socket.disconnect();
    };
  }, []); // Empty dependency array ensures this runs once

  useEffect(() => {
    if (selectedLanguage !== 'all' && socketRef.current) {
      setLoading(true);
      socketRef.current.emit('getTopEmojisForLanguage', selectedLanguage);
    } else if (emojiStats) {
      setCurrentEmojis(emojiStats.topEmojis);
    }
  }, [selectedLanguage, emojiStats]);

  const handleLanguageSelect = (language: string) => {
    setSelectedLanguage(language);
  };

  return (
    <div className="flex flex-col h-screen text-white">
      <Header />
      <LanguageTabs
        languages={languageStats}
        selectedLanguage={selectedLanguage}
        onSelect={handleLanguageSelect}
      />
      <EmojiGrid topEmojis={currentEmojis} />
      <Footer stats={emojiStats || {
        processedPosts: 0,
        processedEmojis: 0,
        postsWithEmojis: 0,
        postsWithoutEmojis: 0,
        ratio: 'N/A',
        topEmojis: [],
      }} />
      {loading && <div>Loading...</div>}
    </div>
  );
}

export default App;
