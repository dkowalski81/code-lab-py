"""
Query Azure Container Apps metrics for multiple apps and plot them on the same graph.

Dependencies:
    uv pip install azure-monitor-query azure-identity matplotlib pandas

Auth:
    Uses DefaultAzureCredential — runs az login once on your machine, no keys needed.

Usage:
    Edit SUBSCRIPTION, RESOURCE_GROUP, and APP_NAMES below, then:
        python azure/container_apps_metrics.py

Common metric names (Microsoft.App/containerApps):
    CpuUsageNanoCores       CPU usage
    MemoryWorkingSetBytes   Memory
    Requests                HTTP request count
    RequestDuration         HTTP request latency (ms)
    ReplicaCount            Number of running replicas
"""

from datetime import timedelta

import matplotlib.pyplot as plt
import pandas as pd
from azure.identity import DefaultAzureCredential
from azure.monitor.query import MetricAggregationType, MetricsQueryClient

# --- configure here ---
SUBSCRIPTION = "your-subscription-id"
RESOURCE_GROUP = "your-rg"
APP_NAMES = ["app-one", "app-two", "app-three"]
METRIC = "CpuUsageNanoCores"
LOOKBACK_HOURS = 1
GRANULARITY_MINUTES = 5
# ----------------------

METRIC_NAMESPACE = "Microsoft.App/containerApps"


def build_resource_ids(subscription: str, resource_group: str, app_names: list[str]) -> list[str]:
    base = f"/subscriptions/{subscription}/resourceGroups/{resource_group}/providers/Microsoft.App/containerApps"
    return [f"{base}/{name}" for name in app_names]


def fetch_metrics(
    resource_ids: list[str],
    metric: str,
    lookback_hours: int,
    granularity_minutes: int,
) -> dict[str, pd.DataFrame]:
    client = MetricsQueryClient(DefaultAzureCredential())

    response = client.query_resources(
        resource_ids=resource_ids,
        metric_names=[metric],
        metric_namespace=METRIC_NAMESPACE,
        timespan=timedelta(hours=lookback_hours),
        granularity=timedelta(minutes=granularity_minutes),
        aggregations=[MetricAggregationType.AVERAGE],
    )

    series: dict[str, pd.DataFrame] = {}
    for result in response:
        app_name = result.resource_id.split("/")[-1]
        rows = [
            {"time": point.timestamp, "value": point.average}
            for m in result.metrics
            for ts in m.timeseries
            for point in ts.data
        ]
        series[app_name] = pd.DataFrame(rows).set_index("time") if rows else pd.DataFrame()

    return series


def list_available_metrics(resource_id: str) -> None:
    client = MetricsQueryClient(DefaultAzureCredential())
    definitions = client.list_metric_definitions(resource_id, namespace=METRIC_NAMESPACE)
    for d in definitions:
        print(f"  {d.name}  ({d.display_description})")


def plot(series: dict[str, pd.DataFrame], metric: str) -> None:
    df = pd.concat(series, axis=1)
    df.columns = [app for app, _ in df.columns]  # flatten MultiIndex

    df.plot(title=f"{metric} by Container App", figsize=(12, 5))
    plt.ylabel(metric)
    plt.xlabel("Time (UTC)")
    plt.tight_layout()
    plt.show()


def main() -> None:
    resource_ids = build_resource_ids(SUBSCRIPTION, RESOURCE_GROUP, APP_NAMES)

    # Uncomment to list all available metrics for one app:
    # list_available_metrics(resource_ids[0])

    print(f"Querying {METRIC} for {len(APP_NAMES)} apps (last {LOOKBACK_HOURS}h)...")
    series = fetch_metrics(resource_ids, METRIC, LOOKBACK_HOURS, GRANULARITY_MINUTES)

    for app_name, df in series.items():
        print(f"\n{app_name}:")
        print(df.to_string() if not df.empty else "  no data")

    if any(not df.empty for df in series.values()):
        plot(series, METRIC)
    else:
        print("No data returned — check app names, subscription, and time range.")


if __name__ == "__main__":
    main()
