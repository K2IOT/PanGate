-- push-api spec §3.2. KEYS: runids{seqepochs}, seqepochs (same slot).
-- ARGV: range "<start>-<end>", runId, start, end, prevKnown (run_id this pod saw last, or ""), floor (highest epoch
-- this pod knew in the range). Returns 1 when the range's epochs were bumped.
local stored = redis.call('HGET', KEYS[1], ARGV[1])
if stored == ARGV[2] then return 0 end
if (not stored) and (ARGV[5] == '' or ARGV[5] == ARGV[2]) then
  redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])   -- first sight of this range (or resharding): no bump
  return 0
end
-- run_id changed, or the hash was lost with the data while this pod remembers another run_id
redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
local floor = tonumber(ARGV[6])
for s = tonumber(ARGV[3]), tonumber(ARGV[4]) do
  local cur = tonumber(redis.call('HGET', KEYS[2], tostring(s)) or '0')
  if cur < floor then cur = floor end
  redis.call('HSET', KEYS[2], tostring(s), tostring(cur + 1))
end
return 1
