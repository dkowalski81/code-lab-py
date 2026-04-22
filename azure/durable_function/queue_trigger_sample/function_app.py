import asyncio
import json
import logging
import os

import azure.functions as func
import azure.durable_functions as df
import httpx

logger = logging.getLogger(__name__)

ENDPOINT_AAA_URL = os.getenv("ENDPOINT_AAA_URL", "http://localhost:9000/aaa")

app = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)


# ---------------------------------------------------------------------------
# Queue trigger — receives the initial message and starts the orchestration
# ---------------------------------------------------------------------------

@app.queue_trigger(
    arg_name="msg",
    queue_name="%TRIGGER_QUEUE_NAME%",
    connection="AzureWebJobsStorage",
)
@app.durable_client_input(client_name="client")
async def queue_trigger(msg: func.QueueMessage, client: df.DurableOrchestrationClient):
    payload = json.loads(msg.get_body().decode())
    logger.info("Queue trigger received: %s", payload)

    instance_id = await client.start_new("orchestrator", None, payload)
    logger.info("Started orchestration instance: %s", instance_id)


# ---------------------------------------------------------------------------
# Orchestrator — calls Act1 → Act2 → ActFinal in sequence
# ---------------------------------------------------------------------------

@app.orchestration_trigger(context_name="context")
def orchestrator(context: df.DurableOrchestrationContext):
    input_data = context.get_input()

    if not context.is_replaying:
        logger.info("Orchestrator started with input: %s", input_data)

    result1 = yield context.call_activity("act1", input_data)

    result2 = yield context.call_activity("act2", result1)

    final_result = yield context.call_activity("act_final", result2)

    return final_result


# ---------------------------------------------------------------------------
# Activity: Act1
# ---------------------------------------------------------------------------

@app.activity_trigger(input_name="data")
async def act1(data: dict) -> dict:
    logger.info("[Act1] Starting with: %s", data)
    print(f"[Act1] received data: {data}")

    await asyncio.sleep(2)  # simulate work

    result = {**data, "act1_result": "act1_processed"}
    print(f"[Act1] done, result: {result}")
    return result


# ---------------------------------------------------------------------------
# Activity: Act2
# ---------------------------------------------------------------------------

@app.activity_trigger(input_name="data")
async def act2(data: dict) -> dict:
    logger.info("[Act2] Starting with: %s", data)
    print(f"[Act2] received data: {data}")

    await asyncio.sleep(2)  # simulate work

    result = {**data, "act2_result": "act2_processed"}
    print(f"[Act2] done, result: {result}")
    return result


# ---------------------------------------------------------------------------
# Activity: ActFinal — calls endpoint AAA
# ---------------------------------------------------------------------------

@app.activity_trigger(input_name="data")
async def act_final(data: dict) -> dict:
    logger.info("[ActFinal] Calling endpoint AAA with: %s", data)

    async with httpx.AsyncClient(timeout=30) as client:
        response = await client.post(ENDPOINT_AAA_URL, json=data)
        response.raise_for_status()
        result = response.json()

    logger.info("[ActFinal] Endpoint AAA responded: %s", result)
    return result
