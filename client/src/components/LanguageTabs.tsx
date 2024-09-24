import React from 'react';

interface LanguageStat {
  language: string;
  count: number;
}

interface LanguageTabsProps {
  languages: LanguageStat[];
  selectedLanguage: string;
  onSelect: (language: string) => void;
}

const LanguageTabs: React.FC<LanguageTabsProps> = ({ languages, selectedLanguage, onSelect }) => {
  return (
    <div className="flex space-x-4 p-4 bg-gray-800">
      <button
        className={`px-4 py-2 rounded ${
          selectedLanguage === 'all' ? 'bg-blue-500 text-white' : 'bg-gray-700 text-gray-300'
        }`}
        onClick={() => onSelect('all')}
      >
        All
      </button>
      {languages.map((lang) => (
        <button
          key={lang.language}
          className={`px-4 py-2 rounded ${
            selectedLanguage === lang.language ? 'bg-blue-500 text-white' : 'bg-gray-700 text-gray-300'
          }`}
          onClick={() => onSelect(lang.language)}
        >
          {lang.language} ({lang.count})
        </button>
      ))}
    </div>
  );
};

export default LanguageTabs;
