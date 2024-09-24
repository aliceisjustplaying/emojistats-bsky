import React from 'react';
import * as Tabs from '@radix-ui/react-tabs';

interface LanguageStat {
  language: string;
  count: number;
}

interface LanguageTabsProps {
  languages: LanguageStat[];
  selectedLanguage: string;
  onSelect: (language: string) => void;
  totalEmojiCount: number;
}

const LanguageTabs: React.FC<LanguageTabsProps> = ({ languages, selectedLanguage, onSelect, totalEmojiCount }) => {
  return (
    <Tabs.Root
      value={selectedLanguage}
      onValueChange={onSelect}
      className="w-full bg-white shadow-md rounded-t-lg"
    >
      <Tabs.List className="flex border-t border-l border-r border-gray-300">
      <Tabs.Trigger
  value="all"
  className={`px-6 py-2 border-t border-l border-r ${
    selectedLanguage === 'all'
      ? 'border-blue-500 text-blue-600 rounded-t-lg'
      : 'border-transparent text-gray-600 hover:text-blue-500'
  } focus:outline-none focus:ring-2 focus:ring-blue-400`}
>
  All ({totalEmojiCount})
</Tabs.Trigger>
        {languages.map((lang) => (
          <Tabs.Trigger
            key={lang.language}
            value={lang.language}
            className={`px-6 py-2 border-t border-l border-r ${
              selectedLanguage === lang.language
                ? 'border-blue-500 text-blue-600 rounded-t-lg'
                : 'border-transparent text-gray-600 hover:text-blue-500'
            } focus:outline-none focus:ring-2 focus:ring-blue-400`}
          >
            {lang.language} ({lang.count})
          </Tabs.Trigger>
        ))}
      </Tabs.List>
      {/* Hidden Tabs.Content for accessibility */}
      <Tabs.Content value={selectedLanguage} className="hidden">
        {/* Content is managed by App.tsx */}
      </Tabs.Content>
    </Tabs.Root>
  );
};

export default LanguageTabs;
