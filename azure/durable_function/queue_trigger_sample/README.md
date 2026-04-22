 Key points:

  - queue_trigger (function_app.py:16) — decodes the queue message and calls client.start_new("orchestrator", ...)
  - orchestrator (function_app.py:33) — a plain generator function (required by Durable Functions); calls Act1 → Act2 → ActFinal with yield
  - act1 / act2 — async, print the input, asyncio.sleep(2) to simulate work, return the input dict with an extra key
  - act_final — POSTs the accumulated result to ENDPOINT_AAA_URL via httpx

  To run locally:

  # start Azurite
  azurite --silent

  # install deps and start the host
  cd azure_durable
  pip install -r requirements.txt
  func start

  One thing to note: the orchestrator function must be synchronous (a generator, not async def) — that's a Durable Functions constraint because
  the framework replays it. Activities can be async def.