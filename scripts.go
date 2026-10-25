package redisqueue

import "github.com/redis/go-redis/v9"

const enqueueLua = `
if redis.call("HEXISTS", KEYS[1], ARGV[1]) == 1 then
	return {0, "exists"}
end
redis.call("HSET", KEYS[1], ARGV[1], ARGV[2])
redis.call("HSET", KEYS[2], ARGV[1], ARGV[3])
redis.call("HSET", KEYS[3], ARGV[1], 0)
redis.call("HSET", KEYS[4], ARGV[1], ARGV[4])
redis.call("HSET", KEYS[5], ARGV[1], ARGV[4])
redis.call("RPUSH", ARGV[5], ARGV[1])
redis.call("ZADD", KEYS[6], -tonumber(ARGV[3]), ARGV[3])
return {1, "queued"}
`

const reserveLua = `
local now = tonumber(ARGV[1])
local visibility = tonumber(ARGV[2])
local ready_prefix = ARGV[3]
for i = 1, 100 do
	local bucket = redis.call("ZRANGE", KEYS[1], 0, 0)[1]
	if not bucket then
		return nil
	end
	local list_key = ready_prefix .. bucket
	local id = redis.call("LPOP", list_key)
	if redis.call("LLEN", list_key) == 0 then
		redis.call("ZREM", KEYS[1], bucket)
	end
	if id then
		local payload = redis.call("HGET", KEYS[4], id)
		local priority = redis.call("HGET", KEYS[5], id)
		if payload and priority then
			local attempts = redis.call("HINCRBY", KEYS[6], id, 1)
			local deadline = now + visibility
			redis.call("LPUSH", KEYS[2], id)
			redis.call("ZADD", KEYS[3], deadline, id)
			redis.call("HSET", KEYS[7], id, now)
			redis.call("HSET", KEYS[8], id, ARGV[4])
			return {id, payload, priority, attempts, deadline, ARGV[4]}
		end
		redis.call("HDEL", KEYS[4], id)
		redis.call("HDEL", KEYS[5], id)
		redis.call("HDEL", KEYS[6], id)
		redis.call("HDEL", KEYS[7], id)
	end
end
return nil
`

const ackLua = `
local id = ARGV[1]
local receipt = redis.call("HGET", KEYS[10], id)
if not receipt then return 0 end
if receipt ~= ARGV[2] then return -1 end
local removed = redis.call("LREM", KEYS[1], 0, id)
local indexed = redis.call("ZREM", KEYS[2], id)
if removed > 0 or indexed > 0 then
	redis.call("HDEL", KEYS[3], id)
	redis.call("HDEL", KEYS[4], id)
	redis.call("HDEL", KEYS[5], id)
	redis.call("HDEL", KEYS[6], id)
	redis.call("HDEL", KEYS[7], id)
	redis.call("HDEL", KEYS[8], id)
	redis.call("HDEL", KEYS[9], id)
	redis.call("HDEL", KEYS[10], id)
	return 1
end
return 0
`

const nackLua = `
local id = ARGV[1]
local receipt = redis.call("HGET", KEYS[12], id)
if not receipt then return {0, "not_found", 0} end
if receipt ~= ARGV[7] then return {-1, "stale", 0} end
local now = tonumber(ARGV[2])
local reason = ARGV[3]
local max_attempts = tonumber(ARGV[4])
local should_requeue = ARGV[5]
local ready_prefix = ARGV[6]
local removed = redis.call("LREM", KEYS[1], 0, id)
local indexed = redis.call("ZREM", KEYS[2], id)
if removed == 0 and indexed == 0 then
	return {0, "not_found", 0}
end
redis.call("HDEL", KEYS[12], id)
local payload = redis.call("HGET", KEYS[3], id)
local priority = redis.call("HGET", KEYS[4], id)
local attempts = tonumber(redis.call("HGET", KEYS[5], id) or "0")
if not payload or not priority then
	redis.call("HDEL", KEYS[3], id)
	redis.call("HDEL", KEYS[4], id)
	redis.call("HDEL", KEYS[5], id)
	redis.call("HDEL", KEYS[6], id)
	return {0, "missing", attempts}
end
if should_requeue == "0" or attempts >= max_attempts then
	redis.call("RPUSH", KEYS[8], id)
	redis.call("ZADD", KEYS[9], now, id)
	redis.call("HSET", KEYS[10], id, reason)
	redis.call("HSET", KEYS[11], id, now)
	redis.call("HSET", KEYS[6], id, now)
	return {1, "dead", attempts}
end
redis.call("RPUSH", ready_prefix .. priority, id)
redis.call("ZADD", KEYS[7], -tonumber(priority), priority)
redis.call("HSET", KEYS[6], id, now)
return {1, "requeued", attempts}
`

const reclaimLua = `
local now = tonumber(ARGV[1])
local limit = tonumber(ARGV[2])
local max_attempts = tonumber(ARGV[3])
local ready_prefix = ARGV[4]
local reason = ARGV[5]
local ids = redis.call("ZRANGEBYSCORE", KEYS[2], "-inf", now, "LIMIT", 0, limit)
local out = {}
for _, id in ipairs(ids) do
	local removed = redis.call("LREM", KEYS[1], 0, id)
	redis.call("ZREM", KEYS[2], id)
	redis.call("HDEL", KEYS[12], id)
	if removed > 0 then
		local payload = redis.call("HGET", KEYS[3], id)
		local priority = redis.call("HGET", KEYS[4], id)
		local attempts = tonumber(redis.call("HGET", KEYS[5], id) or "0")
		if payload and priority then
			if attempts >= max_attempts then
				redis.call("RPUSH", KEYS[8], id)
				redis.call("ZADD", KEYS[9], now, id)
				redis.call("HSET", KEYS[10], id, reason)
				redis.call("HSET", KEYS[11], id, now)
				redis.call("HSET", KEYS[6], id, now)
				table.insert(out, id)
				table.insert(out, "dead")
				table.insert(out, attempts)
			else
				redis.call("RPUSH", ready_prefix .. priority, id)
				redis.call("ZADD", KEYS[7], -tonumber(priority), priority)
				redis.call("HSET", KEYS[6], id, now)
				table.insert(out, id)
				table.insert(out, "requeued")
				table.insert(out, attempts)
			end
		else
			redis.call("HDEL", KEYS[3], id)
			redis.call("HDEL", KEYS[4], id)
			redis.call("HDEL", KEYS[5], id)
			redis.call("HDEL", KEYS[6], id)
			table.insert(out, id)
			table.insert(out, "missing")
			table.insert(out, attempts)
		end
	end
end
return out
`

const retryDeadLua = `
local id = ARGV[1]
local now = tonumber(ARGV[2])
local ready_prefix = ARGV[3]
local priority_override = ARGV[4]
local reset_attempts = ARGV[5]
local removed = redis.call("LREM", KEYS[1], 0, id)
local indexed = redis.call("ZREM", KEYS[2], id)
if removed == 0 and indexed == 0 then
	return 0
end
local payload = redis.call("HGET", KEYS[3], id)
if not payload then
	return 0
end
local priority = redis.call("HGET", KEYS[4], id)
if priority_override ~= "" then
	priority = priority_override
	redis.call("HSET", KEYS[4], id, priority)
end
if not priority then
	return 0
end
if reset_attempts == "1" then
	redis.call("HSET", KEYS[5], id, 0)
end
redis.call("HDEL", KEYS[7], id)
redis.call("HDEL", KEYS[8], id)
redis.call("HSET", KEYS[6], id, now)
redis.call("RPUSH", ready_prefix .. priority, id)
redis.call("ZADD", KEYS[9], -tonumber(priority), priority)
return 1
`

const statsLua = `
local ready = 0
local buckets = redis.call("ZRANGE", KEYS[1], 0, -1)
for _, bucket in ipairs(buckets) do
	ready = ready + redis.call("LLEN", ARGV[1] .. bucket)
end
return {
	ready,
	#buckets,
	redis.call("LLEN", KEYS[2]),
	redis.call("ZCARD", KEYS[3]),
	redis.call("LLEN", KEYS[4]),
	redis.call("ZCARD", KEYS[5]),
	redis.call("HLEN", KEYS[6])
}
`

const inspectTaskLua = `
local id = ARGV[1]
local include_payload = ARGV[2]
local ready_prefix = ARGV[3]
local payload = redis.call("HGET", KEYS[1], id)
local priority = redis.call("HGET", KEYS[2], id)
local attempts = redis.call("HGET", KEYS[3], id) or "0"
local enqueued = redis.call("HGET", KEYS[4], id) or "0"
local updated = redis.call("HGET", KEYS[5], id) or "0"
local deadline = redis.call("ZSCORE", KEYS[7], id) or "0"
local dead_at = redis.call("HGET", KEYS[10], id) or "0"
local reason = redis.call("HGET", KEYS[9], id) or ""
local status = "done"
local ready_pos = -1
if priority then
	local ready_key = ready_prefix .. priority
	local ready_items = redis.call("LRANGE", ready_key, 0, -1)
	for i, item in ipairs(ready_items) do
		if item == id then
			status = "ready"
			ready_pos = i - 1
			break
		end
	end
end
if status == "done" and redis.call("ZSCORE", KEYS[7], id) then
	status = "working"
end
if redis.call("ZSCORE", KEYS[8], id) then
	status = "dead"
end
local exists = 0
if payload or priority or status ~= "done" then
	exists = 1
end
if exists == 1 and not payload then
	status = "orphan"
end
local payload_out = ""
if include_payload == "1" and payload then
	payload_out = payload
end
return {
	status,
	exists,
	payload and 1 or 0,
	payload_out,
	priority or "0",
	attempts,
	enqueued,
	updated,
	deadline,
	dead_at,
	reason,
	ready_pos
}
`

const acquireConcurrencyLua = `
local now = tonumber(ARGV[1])
local deadline = tonumber(ARGV[2])
local token = ARGV[3]
local service_max = tonumber(ARGV[4])
local total_max = tonumber(ARGV[5])
redis.call("ZREMRANGEBYSCORE", KEYS[1], "-inf", now)
redis.call("ZREMRANGEBYSCORE", KEYS[2], "-inf", now)
if total_max > 0 and redis.call("ZCARD", KEYS[1]) >= total_max then
	return {0, "total"}
end
if service_max > 0 and redis.call("ZCARD", KEYS[2]) >= service_max then
	return {0, "service"}
end
if total_max > 0 then
	redis.call("ZADD", KEYS[1], deadline, token)
end
if service_max > 0 then
	redis.call("ZADD", KEYS[2], deadline, token)
end
return {1, "acquired"}
`

const renewConcurrencyLua = `
local token = ARGV[1]
local deadline = tonumber(ARGV[2])
local use_service = ARGV[3]
local use_total = ARGV[4]
if use_total == "1" then
	if not redis.call("ZSCORE", KEYS[1], token) then
		return 0
	end
	redis.call("ZADD", KEYS[1], deadline, token)
end
if use_service == "1" then
	if not redis.call("ZSCORE", KEYS[2], token) then
		return 0
	end
	redis.call("ZADD", KEYS[2], deadline, token)
end
return 1
`

const releaseConcurrencyLua = `
local token = ARGV[1]
redis.call("ZREM", KEYS[1], token)
redis.call("ZREM", KEYS[2], token)
return 1
`

var scripts = struct {
	enqueue   *redis.Script
	reserve   *redis.Script
	ack       *redis.Script
	nack      *redis.Script
	reclaim   *redis.Script
	retryDead *redis.Script
	stats     *redis.Script
	acquire   *redis.Script
	renew     *redis.Script
	release   *redis.Script
	inspect   *redis.Script
}{
	enqueue:   redis.NewScript(enqueueLua),
	reserve:   redis.NewScript(reserveLua),
	ack:       redis.NewScript(ackLua),
	nack:      redis.NewScript(nackLua),
	reclaim:   redis.NewScript(reclaimLua),
	retryDead: redis.NewScript(retryDeadLua),
	stats:     redis.NewScript(statsLua),
	acquire:   redis.NewScript(acquireConcurrencyLua),
	renew:     redis.NewScript(renewConcurrencyLua),
	release:   redis.NewScript(releaseConcurrencyLua),
	inspect:   redis.NewScript(inspectTaskLua),
}
