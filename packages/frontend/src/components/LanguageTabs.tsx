import React from 'react';
import { Tab, TabList, TabPanel, Tabs } from 'react-tabs';
import 'react-tabs/style/react-tabs.css';

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

const LanguageTabs: React.FC<LanguageTabsProps> = ({ languages, onSelect, totalEmojiCount }) => {
  return (
    <Tabs
      defaultIndex={0}
      onSelect={(index) => {
        if (index === 0) {
          onSelect('all');
        } else {
          onSelect(languages[index - 1].language);
        }
      }}
    >
      <div className="tablist-container">
        <TabList>
          <Tab key="all" value="all">
            All ({totalEmojiCount})
          </Tab>
          {languages.map((lang) => (
            <Tab key={lang.language} value={lang.language}>
              {lang.language.toLowerCase()} ({lang.count})
            </Tab>
          ))}
        </TabList>
      </div>
      <TabPanel key="all"></TabPanel>
      {languages.map((lang) => (
        <TabPanel key={lang.language}>{null}</TabPanel>
      ))}
    </Tabs>
  );
};

export default LanguageTabs;
