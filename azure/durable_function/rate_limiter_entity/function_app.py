"""
Rate limiting with a Durable Entity.

The entity holds a token bucket (fixed capacity). Activities acquire a token
before calling the throttled resource and release it when done.

No extra infrastructure needed — state lives inside the Durable Functions runtime.
"""

import asyncio
import logging

import azure.functions as func
import azure.durable_functions as df

logger = logging.getLogger(__name__)

CAPACITY = 5          # max concurrent calls to the throttled resource
app = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)
ENTITY_KEY = "global"


# ---------------------------------------------------------------------------
# Entity — single-threaded token bucket (acquire / release)
# ---------------------------------------------------------------------------

@app.entity_trigger(context_name="ctx")
def rate_limiter(ctx: df.DurableEntityContext):
    tokens: int = ctx.get_state(lambda: CAPACITY)

    if ctx.operation_name == "acquire":
        if tokens > 0:
            ctx.set_state(tokens - 1)
            ctx.set_result(True)
        else:
            ctx.set_result(False)

    elif ctx.operation_name == "release":
        ctx.set_state(min(CAPACITY, tokens + 1))


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
# Activity — acquires a token, calls the resource, releases the token
# ---------------------------------------------------------------------------

@app.activity_trigger(input_name="item")
async def process_item(item: str, context: df.DurableOrchestrationContext) -> dict:
    entity_id = df.EntityId("rate_limiter", ENTITY_KEY)

    # poll until a token is available
    while True:
        acquired: bool = await context.call_entity(entity_id, "acquire")
        if acquired:
            break
        await asyncio.sleep(0.5)

    try:
        result = await _call_throttled_resource(item)
    finally:
        await context.call_entity(entity_id, "release")

    return result


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
    items = [f"doc_{i}" for i in range(10)]
    instance_id = await client.start_new("orchestrator", None, items)
    return client.create_check_status_response(req, instance_id)
