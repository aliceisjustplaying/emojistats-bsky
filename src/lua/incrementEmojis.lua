local emoji = ARGV[1]
local langKeys = cjson.decode(ARGV[2])

-- Increment global counters
redis.call('INCR', 'postsWithEmojis')
redis.call('ZINCRBY', 'emojiStats', 1, emoji)
redis.call('INCR', 'processedEmojis')

-- Increment per-language emoji counts and global language stats
for i, langKey in ipairs(langKeys) do
  redis.call('ZINCRBY', langKey, 1, emoji)
  redis.call('ZINCRBY', 'languageStats', 1, langKey)
end

return 'OK'
