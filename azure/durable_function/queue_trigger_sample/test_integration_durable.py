"""
Integration test skeleton for the Azure Durable Functions pipeline:
  QueueTrigger -> Orchestrator -> Act1 -> Act2 -> ActFinal -> endpoint AAA

Prerequisites (run before this test):
  - Azure Functions host running locally:  func start
  - Azurite (local emulator) running:      azurite --silent
  - OR real Azure credentials in env vars (see CONFIGURATION below)

Dependencies:
  pip install pytest pytest-asyncio azure-storage-queue responses httpx
"""

import json
import os
import time
import uuid

import httpx
import pytest
from azure.storage.queue import QueueClient

# ---------------------------------------------------------------------------
# CONFIGURATION — override via environment variables or a .env file
# ---------------------------------------------------------------------------

STORAGE_CONNECTION_STRING = os.getenv(
    "AZURE_STORAGE_CONNECTION_STRING",
    "UseDevelopmentStorage=true",  # Azurite default
)
QUEUE_NAME = os.getenv("TRIGGER_QUEUE_NAME", "your-trigger-queue-name")

# Base URL of the running Functions host
FUNCTIONS_BASE_URL = os.getenv("FUNCTIONS_BASE_URL", "http://localhost:7071")

# Endpoint AAA that ActFinal calls — set to your mock/stub server in tests
ENDPOINT_AAA_URL = os.getenv("ENDPOINT_AAA_URL", "http://localhost:9000/aaa")

# How long (seconds) to wait for the orchestration to finish
ORCHESTRATION_TIMEOUT_SEC = int(os.getenv("ORCHESTRATION_TIMEOUT_SEC", "60"))
POLL_INTERVAL_SEC = 2


# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------


def enqueue_message(payload: dict) -> str:
    """Put a JSON message on the trigger queue and return a correlation id."""
    correlation_id = str(uuid.uuid4())
    payload.setdefault("correlationId", correlation_id)

    client = QueueClient.from_connection_string(STORAGE_CONNECTION_STRING, QUEUE_NAME)
    client.send_message(json.dumps(payload))
    return correlation_id


def poll_orchestration_status(
    status_query_url: str,
    timeout: int = ORCHESTRATION_TIMEOUT_SEC,
    interval: int = POLL_INTERVAL_SEC,
) -> dict:
    """
    Poll the Durable Functions management endpoint until the orchestration
    reaches a terminal state (Completed / Failed / Terminated / Canceled).

    status_query_url is the `statusQueryGetUri` returned when the
    orchestration is started, e.g.:
      http://localhost:7071/runtime/webhooks/durabletask/instances/<id>?...
    """
    terminal_states = {"Completed", "Failed", "Terminated", "Canceled"}
    deadline = time.monotonic() + timeout

    while time.monotonic() < deadline:
        response = httpx.get(status_query_url, timeout=10)
        response.raise_for_status()
        status = response.json()
        if status.get("runtimeStatus") in terminal_states:
            return status
        time.sleep(interval)

    raise TimeoutError(
        f"Orchestration did not complete within {timeout}s. "
        f"Last status: {status.get('runtimeStatus')}"
    )


def find_orchestration_instance(correlation_id: str, timeout: int = 30) -> str:
    """
    Locate the Durable Functions management URL for the orchestration
    instance that corresponds to our correlation id.

    Strategy: poll the instances list endpoint until an instance whose
    input contains our correlation_id appears.  Adjust the filter/logic
    to match however your orchestrator stores the id.
    """
    instances_url = (
        f"{FUNCTIONS_BASE_URL}/runtime/webhooks/durabletask/instances"
    )
    deadline = time.monotonic() + timeout

    while time.monotonic() < deadline:
        response = httpx.get(instances_url, timeout=10)
        response.raise_for_status()
        for instance in response.json():
            input_data = instance.get("input") or {}
            if isinstance(input_data, str):
                input_data = json.loads(input_data)
            if input_data.get("correlationId") == correlation_id:
                return instance["statusQueryGetUri"]
        time.sleep(POLL_INTERVAL_SEC)

    raise TimeoutError(
        f"No orchestration instance found for correlationId={correlation_id} "
        f"within {timeout}s"
    )


# ---------------------------------------------------------------------------
# FIXTURES
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def aaa_stub(httpserver):
    """
    Stub for endpoint AAA using pytest-localserver (or swap for `responses`
    if you prefer a requests-based mock).

    Install:  pip install pytest-localserver
    """
    # Record calls so we can assert on them later
    aaa_stub.received_requests = []

    def handler(request):
        from werkzeug.wrappers import Response
        aaa_stub.received_requests.append(json.loads(request.data))
        return Response(json.dumps({"status": "ok"}), content_type="application/json")

    httpserver.expect_request("/aaa").respond_with_handler(handler)
    yield httpserver


# ---------------------------------------------------------------------------
# TESTS
# ---------------------------------------------------------------------------


class TestDurablePipeline:
    """End-to-end integration tests for the durable orchestration pipeline."""

    def test_happy_path(self, aaa_stub):
        """
        A valid queue message should:
          1. Trigger the orchestrator
          2. Execute Act1 then Act2
          3. Call ActFinal, which POSTs to endpoint AAA
          4. Orchestration finishes with status Completed
        """
        # ---- Arrange -------------------------------------------------------
        payload = {
            "field1": "value1",  # TODO: replace with realistic trigger payload
            "field2": 42,
        }

        # ---- Act -----------------------------------------------------------
        correlation_id = enqueue_message(payload)
        status_url = find_orchestration_instance(correlation_id)
        result = poll_orchestration_status(status_url)

        # ---- Assert — orchestration outcome --------------------------------
        assert result["runtimeStatus"] == "Completed", (
            f"Expected Completed, got {result['runtimeStatus']}. "
            f"Output: {result.get('output')}"
        )

        # ---- Assert — endpoint AAA was called ------------------------------
        assert len(aaa_stub.received_requests) == 1, (
            "ActFinal should have called endpoint AAA exactly once"
        )
        aaa_request = aaa_stub.received_requests[0]
        # TODO: assert on the shape/content of the AAA payload
        assert "correlationId" in aaa_request  # example assertion

        # ---- Assert — orchestration output (optional) ----------------------
        output = result.get("output")
        # TODO: assert on whatever ActFinal/orchestrator returns
        assert output is not None

    def test_act1_failure_propagates(self):
        """
        If Act1 fails (e.g. bad input), the orchestration should reach
        a Failed state and NOT call endpoint AAA.
        """
        payload = {
            "field1": None,  # TODO: replace with a payload that causes Act1 to fail
        }

        correlation_id = enqueue_message(payload)
        status_url = find_orchestration_instance(correlation_id)
        result = poll_orchestration_status(status_url)

        assert result["runtimeStatus"] == "Failed"
        # TODO: assert aaa_stub received 0 calls (or check side-effects)

    def test_act2_result_fed_to_actfinal(self):
        """
        Verify that the output of Act2 is correctly forwarded to ActFinal
        and ultimately reflected in the AAA payload.
        """
        payload = {
            "field1": "expected_value",
        }

        correlation_id = enqueue_message(payload)
        status_url = find_orchestration_instance(correlation_id)
        result = poll_orchestration_status(status_url)

        assert result["runtimeStatus"] == "Completed"
        # TODO: inspect aaa_stub.received_requests[0] for Act2's output


# ---------------------------------------------------------------------------
# ENTRY POINT (optional, for direct execution)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys
    sys.exit(pytest.main([__file__, "-v"]))
