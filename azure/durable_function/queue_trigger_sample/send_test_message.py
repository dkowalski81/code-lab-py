"""Send a test JSON message to the Azure Storage Queue (Azurite or real storage)."""

import json
import os
import base64
from azure.core.exceptions import ResourceExistsError
from azure.storage.queue import QueueClient

_AZURITE = (
    "DefaultEndpointsProtocol=http;"
    "AccountName=devstoreaccount1;"
    "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tiqp;"
    "QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;"
)
queue_name = 'my-test-queue'

conn_str = os.getenv("AzureWebJobsStorage", "UseDevelopmentStorage=true")
#if conn_str == "UseDevelopmentStorage=true":
#    conn_str = _AZURITE

payload = {
    "id": "test-001",
    "message": "hello from send_test_message.py",
}

client = QueueClient.from_connection_string(conn_str, queue_name)

try:
    client.create_queue()
except ResourceExistsError:
    pass

byte_data = json.dumps(payload).encode('utf-8')
b_msg = base64.b64encode(byte_data)
msg = b_msg.decode('utf-8')

client.send_message(msg)
print(f"Sent to '{queue_name}': {payload}\n{msg}")
