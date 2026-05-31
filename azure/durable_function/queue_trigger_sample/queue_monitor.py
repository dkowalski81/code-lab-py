"""
Query Azure Log Analytics for storage queue throughput and length.

Usage:
    python queue_monitor.py --queue <queue-name> --workspace <workspace-guid>
    python queue_monitor.py --queue myqueue --workspace abc123 --hours 6
    python queue_monitor.py --queue myqueue --workspace abc123 --interval 15m
"""
import argparse
import os
from datetime import timedelta

import pandas as pd
from azure.identity import DefaultAzureCredential
from azure.monitor.query import LogsQueryClient, LogsQueryStatus


def query_queue(workspace_id: str, queue_name: str, hours: int, interval: str) -> None:
    client = LogsQueryClient(DefaultAzureCredential())

    throughput_query = f"""
StorageQueueLogs
| where OperationName in ("PutMessage", "DeleteMessage")
| where QueueName == "{queue_name}"
| summarize count() by bin(TimeGenerated, {interval}), OperationName
| order by TimeGenerated asc
"""

    length_query = f"""
AzureMetrics
| where ResourceProvider == "MICROSOFT.STORAGE"
| where MetricName == "QueueMessageCount"
| summarize avg_length = avg(Average) by bin(TimeGenerated, {interval})
| order by TimeGenerated asc
"""

    timespan = timedelta(hours=hours)

    print(f"\n--- Queue throughput: {queue_name} (last {hours}h, interval {interval}) ---")
    resp = client.query_workspace(workspace_id, throughput_query, timespan=timespan)
    if resp.status == LogsQueryStatus.SUCCESS and resp.tables[0].rows:
        df = pd.DataFrame(resp.tables[0].rows, columns=resp.tables[0].columns)
        df["TimeGenerated"] = pd.to_datetime(df["TimeGenerated"]).dt.strftime("%Y-%m-%d %H:%M")
        print(df.to_string(index=False))
    elif resp.status == LogsQueryStatus.PARTIAL:
        print("Warning: partial results returned.")
        if resp.partial_data:
            df = pd.DataFrame(resp.partial_data[0].rows, columns=resp.partial_data[0].columns)
            print(df.to_string(index=False))
    else:
        print("No throughput data found.")

    print(f"\n--- Queue length (storage account level, last {hours}h) ---")
    resp = client.query_workspace(workspace_id, length_query, timespan=timespan)
    if resp.status == LogsQueryStatus.SUCCESS and resp.tables[0].rows:
        df = pd.DataFrame(resp.tables[0].rows, columns=resp.tables[0].columns)
        df["TimeGenerated"] = pd.to_datetime(df["TimeGenerated"]).dt.strftime("%Y-%m-%d %H:%M")
        print(df.to_string(index=False))
    else:
        print("No queue length data found. Check AzureMetrics is flowing to this workspace.")


def main():
    parser = argparse.ArgumentParser(description="Monitor Azure Storage Queue via Log Analytics.")
    parser.add_argument("--queue", required=True, help="Queue name to filter on")
    parser.add_argument(
        "--workspace",
        default=os.environ.get("LOG_ANALYTICS_WORKSPACE_ID"),
        help="Log Analytics workspace GUID (or set LOG_ANALYTICS_WORKSPACE_ID env var)",
    )
    parser.add_argument("--hours", type=int, default=24, help="Lookback window in hours (default: 24)")
    parser.add_argument("--interval", default="1h", help="Aggregation interval, KQL syntax e.g. 5m 1h (default: 1h)")
    args = parser.parse_args()

    if not args.workspace:
        parser.error("--workspace is required (or set LOG_ANALYTICS_WORKSPACE_ID)")

    query_queue(args.workspace, args.queue, args.hours, args.interval)


if __name__ == "__main__":
    main()
