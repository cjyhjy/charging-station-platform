package redis

import (
	"strconv"
	"time"
)

// The two operations that must be atomic are implemented as server-side Lua.
//
// Redis executes a script as a single command, so no other client can observe or
// interleave a state between the steps. This is what removes the failure modes a
// sequence of separate commands cannot close:
//
//   - rate limiting: INCR followed by a separate EXPIRE leaves a permanent
//     counter if the process dies, the reply is lost, or the cleanup fails;
//   - lock release: GET followed by a separate DEL can delete a lock that a
//     different holder acquired in between.
//
// Both scripts take their keys through KEYS so they remain compatible with a
// Redis Cluster deployment later.

// rateLimitScript increments a fixed-window counter and guarantees the window is
// attached, returning {count, ttlMs}.
//
// The "ttl < 0" branch is self-healing: if a counter exists with no expiry - for
// example one written by an older client that used separate commands - the
// window is attached on the next call instead of leaving the identity blocked
// forever.
const rateLimitScript = `
local count = redis.call('INCR', KEYS[1])
local ttl = redis.call('PTTL', KEYS[1])
if count == 1 or ttl < 0 then
  redis.call('PEXPIRE', KEYS[1], ARGV[1])
  ttl = tonumber(ARGV[1])
end
return {count, ttl}
`

// lockReleaseScript deletes a lock only when the caller still owns it.
const lockReleaseScript = `
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
`

// scriptArg renders a duration as whole milliseconds, the smallest unit this
// adapter uses. A sub-millisecond duration rounds up so a window can never
// become a zero-millisecond expiry, which Redis would reject.
func scriptArg(ttl time.Duration) string {
	ms := ttl.Milliseconds()
	if ms <= 0 {
		ms = 1
	}
	return strconv.FormatInt(ms, 10)
}
