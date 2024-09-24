local emoji = ARGV[1]
local langKeys = cjson.decode(ARGV[2])
local isFirstEmoji = ARGV[3]

-- Increment global counters
if isFirstEmoji == "1" then
  redis.call('INCR', 'postsWithEmojis')
end
redis.call('ZINCRBY', 'emojiStats', 1, emoji)
redis.call('INCR', 'processedEmojis')

-- Increment per-language emoji counts and global language stats
for i, langKey in ipairs(langKeys) do
  redis.call('ZINCRBY', langKey, 1, emoji) -- langKey being pt, ja, UNKNOWN, etc.
  redis.call('ZINCRBY', 'languageStats', 1, langKey)
end

return 'OK'
