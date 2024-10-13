local emojis = cjson.decode(ARGV[1]) -- array of emojis
local langs = cjson.decode(ARGV[2]) -- array of languages

-- Increment global counters
redis.call('INCR', 'postsWithEmojis')

for _, emoji in ipairs(emojis) do
  redis.call('ZINCRBY', 'emojiStats', 1, emoji) -- emojiStats is the "all" global counter
  redis.call('INCR', 'processedEmojis')
end

-- Increment per-language emoji counts and global language stats
for _, emoji in ipairs(emojis) do
  for _, lang in ipairs(langs) do
    redis.call('ZINCRBY', lang, 1, emoji) -- langKey being pt, ja, unknown, etc.
    redis.call('ZINCRBY', 'languageStats', 1, lang) -- languageStats is the counter for per-language emoji count
  end
end

return 'OK'
