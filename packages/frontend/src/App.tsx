import { useCallback, useEffect, useRef, useState } from 'react';
import { Socket, io } from 'socket.io-client';
import { useEffectEvent } from 'use-effect-event';

import EmojiGrid from './components/EmojiGrid.js';
import Footer from './components/Footer.js';
import Header from './components/Header.js';
import LanguageTabs from './components/LanguageTabs.js';

type TopEmojis = Array<{
  emoji: string;
  count: number;
}>;

interface EmojiStats {
  processedPosts: number;
  processedEmojis: number;
  postsWithEmojis: number;
  postsWithoutEmojis: number;
  ratio: string;
  topEmojis: TopEmojis;
}

interface LanguageStat {
  language: string;
  count: number;
}

type TopEmojisForLanguage = {
  language: string;
  topEmojis: TopEmojis;
};

const NO_EMOJIS: TopEmojis = [];

function App() {
  const [selectedLanguage, setSelectedLanguage] = useState<string>('all');

  const [languageStats, setLanguageStats] = useState<LanguageStat[]>([]);
  const [emojiStats, setEmojiStats] = useState<EmojiStats | null>(null);

  const [topEmojisCache, setTopEmojisCache] = useState<Record<string, TopEmojis>>({});
  const updateTopEmojiCacheEntry = useCallback((language: string, value: TopEmojis) => {
    return setTopEmojisCache((prev) => ({ ...prev, [language]: value }));
  }, []);

  // TODO: loading state instead of defaulting to an empty array?
  const topEmojis =
    (selectedLanguage === 'all' ? emojiStats?.topEmojis : topEmojisCache[selectedLanguage]) ?? NO_EMOJIS;

  const totalEmojiCount = emojiStats?.processedEmojis ?? 0;

  // listen for data
  // (technically we could avoid using useEffectvent,
  // but it's probably good to avoid future bugs w/ effect deps)
  const onSocketConnected = useEffectEvent((socket: Socket) => {
    // Handle incoming emoji stats
    socket.on('emojiStats', (data: EmojiStats) => {
      setEmojiStats(data);
    });

    // Handle incoming language stats
    socket.on('languageStats', (data: LanguageStat[]) => {
      setLanguageStats(data);
    });

    // Handle incoming top emojis for a specific language
    // NOTE: this only comes in response to a 'getTopEmojisForLanguage' message
    // which we send in an interval if a language is selected
    socket.on('topEmojisForLanguage', (data: TopEmojisForLanguage) => {
      updateTopEmojiCacheEntry(data.language, data.topEmojis);
    });

    socket.on('emojiInfo', (data) => {
      console.log('Emoji Info:', data);
    });
  });

  const socketRef = useRef<Socket | null>(null);
  useEffect(() => {
    const socket = io(import.meta.env.VITE_SOCKET_URL);
    socketRef.current = socket;
    onSocketConnected(socket);

    return () => {
      socket.disconnect();
      socketRef.current = null;
    };
  }, [onSocketConnected]);

  // language selection
  const handleLanguageSelect = useCallback((newLanguage: string) => {
    setSelectedLanguage(newLanguage);
    if (socketRef.current) {
      maybeRequestLanguageData(socketRef.current, newLanguage);
    }
  }, []);

  // we have to repeatedly send 'getTopEmojisForLanguage' messages
  // to get back 'topEmojisForLanguage' data
  useEffect(() => {
    const interval = setInterval(() => {
      if (socketRef.current) {
        maybeRequestLanguageData(socketRef.current, selectedLanguage);
      }
    }, 1000);
    return () => clearInterval(interval);
  }, [selectedLanguage]);

  return (
    <div className="flex flex-col h-full">
      <Header />
      <LanguageTabs
        languages={languageStats}
        selectedLanguage={selectedLanguage}
        onSelect={handleLanguageSelect}
        totalEmojiCount={totalEmojiCount}
      />
      <EmojiGrid
        key={selectedLanguage}
        topEmojis={topEmojis}
        // FIXME: reading ref in render (unnecessary, can just pass callback)
        socket={socketRef.current!}
        lang={selectedLanguage.toLowerCase()}
      />
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

const maybeRequestLanguageData = (socket: Socket, language: string) => {
  if (language !== 'all') {
    socket.emit('getTopEmojisForLanguage', language);
  }
};

export default App;
