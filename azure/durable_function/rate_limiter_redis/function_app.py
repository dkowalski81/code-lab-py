"""
Rate limiting with Redis token bucket (sliding refill).

A Lua script runs atomically on Redis so there are no race conditions across
workers. Each activity acquires a token before hitting the throttled resource.

Requires: pip install redis azure-functions azure-durable-functions
Env var:  REDIS_URL  e.g. rediss://:password@mycache.redis.cache.windows.net:6380
"""

import asyncio
import logging
import os
import time

import redis.asyncio as aioredis
import azure.functions as func
import azure.durable_functions as df

logger = logging.getLogger(__name__)

app = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)

# Token bucket settings
CAPACITY = 20       # maximum tokens in the bucket
REFILL_RATE = 10.0  # tokens added per second

# Lua script: atomically refills based on elapsed time then tries to consume 1 token.
# Returns 1 if acquired, 0 if bucket is empty.
_LUA_SCRIPT = """
local key       = KEYS[1]
local capacity  = tonumber(ARGV[1])
local rate      = tonumber(ARGV[2])
local now       = tonumber(ARGV[3])

local state     = redis.call('HMGET', key, 'tokens', 'ts')
local tokens    = tonumber(state[1]) or capacity
local ts        = tonumber(state[2]) or now

tokens = math.min(capacity, tokens + (now - ts) * rate)

if tokens >= 1 then
    tokens = tokens - 1
    redis.call('HMSET', key, 'tokens', tokens, 'ts', now)
    redis.call('EXPIRE', key, 60)
    return 1
else
    redis.call('HMSET', key, 'tokens', tokens, 'ts', now)
    return 0
end
"""


def _get_redis() -> aioredis.Redis:
    return aioredis.from_url(os.environ["REDIS_URL"], decode_responses=False)


async def acquire_token(r: aioredis.Redis, key: str = "ratelimit:global") -> bool:
    script = r.register_script(_LUA_SCRIPT)
    result = await script(keys=[key], args=[CAPACITY, REFILL_RATE, time.time()])
    return result == 1


# ---------------------------------------------------------------------------
# Orchestrator — fan-out: one activity per item
# ---------------------------------------------------------------------------

@app.orchestration_trigger(context_name="context")
def orchestrator(context: df.DurableOrchestrationContext):
    items: list = context.get_input()
    tasks = [context.call_activity("process_item", item) for item in items]
    results = yield context.task_all(tasks)
    return results


# ---------------------------------------------------------------------------
# Activity — acquires a Redis token then calls the resource
# ---------------------------------------------------------------------------

@app.activity_trigger(input_name="item")
async def process_item(item: str) -> dict:
    async with _get_redis() as r:
        while not await acquire_token(r):
            await asyncio.sleep(0.1)

    return await _call_throttled_resource(item)


async def _call_throttled_resource(item: str) -> dict:
    """Placeholder for your actual API call (e.g. Document Intelligence)."""
    await asyncio.sleep(1)
    return {"item": item, "status": "processed"}


# ---------------------------------------------------------------------------
# HTTP trigger — starts a test orchestration
# ---------------------------------------------------------------------------

@app.route(route="start", methods=["POST"])
@app.durable_client_input(client_name="client")
async def http_start(req: func.HttpRequest, client: df.DurableOrchestrationClient):
    items = [f"doc_{i}" for i in range(20)]
    instance_id = await client.start_new("orchestrator", None, items)
    return client.create_check_status_response(req, instance_id)
