"""
Azure App Configuration – Python SDK example.

Covers:
  - Reading a single setting with exponential-backoff retry on HTTP 429
  - Reading multiple settings in one list call
  - Polling for changes via a sentinel key pattern

Auth: set AZURE_APPCONFIG_CONNECTION_STRING in your environment, or replace
      the client construction with DefaultAzureCredential + an endpoint URL.
"""

import logging
import os
import random
import time
from collections.abc import Generator

from azure.appconfiguration import AzureAppConfigurationClient, ConfigurationSetting
from azure.core.exceptions import HttpResponseError

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Retry helper
# ---------------------------------------------------------------------------

_MAX_RETRIES = 5
_BASE_DELAY = 1.0   # seconds
_MAX_DELAY = 60.0   # seconds


def _call_with_retry(func, *args, **kwargs):
    """Call func; retry on HTTP 429 with exponential back-off + jitter."""
    for attempt in range(_MAX_RETRIES + 1):
        try:
            return func(*args, **kwargs)
        except HttpResponseError as exc:
            if exc.status_code != 429 or attempt == _MAX_RETRIES:
                raise

            # Honour Retry-After if the service sends it.
            retry_after = (exc.response.headers or {}).get("Retry-After")
            if retry_after:
                delay = float(retry_after)
            else:
                delay = min(_BASE_DELAY * (2 ** attempt), _MAX_DELAY)
                delay *= 0.5 + random.random()  # ±50 % jitter

            logger.warning(
                "Rate-limited by App Configuration (attempt %d/%d). "
                "Retrying in %.1f s.",
                attempt + 1, _MAX_RETRIES, delay,
            )
            time.sleep(delay)


# ---------------------------------------------------------------------------
# Reading settings
# ---------------------------------------------------------------------------

def get_setting(
    client: AzureAppConfigurationClient,
    key: str,
    label: str | None = None,
) -> ConfigurationSetting | None:
    """Fetch one setting; returns None when the key does not exist."""
    try:
        return _call_with_retry(client.get_configuration_setting, key=key, label=label)
    except HttpResponseError as exc:
        if exc.status_code == 404:
            return None
        raise


def get_multiple_settings(
    client: AzureAppConfigurationClient,
    key_filter: str = "*",
    label_filter: str | None = None,
) -> list[ConfigurationSetting]:
    """
    Fetch all settings matching key_filter / label_filter in one paged call.

    key_filter supports wildcards, e.g. "myapp/*" or "feature.*".
    list_configuration_settings is lazy (paged iterator), so we materialise
    it inside the retry boundary so a mid-page 429 is also retried.
    """
    def _list():
        return list(
            client.list_configuration_settings(
                key_filter=key_filter,
                label_filter=label_filter,
            )
        )

    return _call_with_retry(_list)


# ---------------------------------------------------------------------------
# Change detection via sentinel key
# ---------------------------------------------------------------------------

def watch_for_changes(
    client: AzureAppConfigurationClient,
    sentinel_key: str,
    sentinel_label: str | None = None,
    poll_interval: float = 30.0,
) -> Generator[ConfigurationSetting, None, None]:
    """
    Yield the updated sentinel ConfigurationSetting whenever it changes.

    The sentinel-key pattern: designate one key (e.g. "app/sentinel") whose
    value or etag you update whenever you deploy a new config batch.  Clients
    only check this one key on every poll tick; they do a full reload only
    when the etag changes — drastically reducing read-unit consumption.

    Usage:
        for change in watch_for_changes(client, "myapp/sentinel"):
            settings = get_multiple_settings(client, key_filter="myapp/*")
            apply_settings(settings)
    """
    sentinel = get_setting(client, sentinel_key, label=sentinel_label)
    last_etag = sentinel.etag if sentinel else None
    logger.info("Watching sentinel '%s' (etag=%s).", sentinel_key, last_etag)

    while True:
        time.sleep(poll_interval)
        try:
            current = get_setting(client, sentinel_key, label=sentinel_label)
            current_etag = current.etag if current else None
            if current_etag != last_etag:
                logger.info(
                    "Sentinel changed: %s → %s", last_etag, current_etag
                )
                last_etag = current_etag
                yield current
        except Exception:
            logger.exception("Error while polling sentinel key.")


# ---------------------------------------------------------------------------
# Demo
# ---------------------------------------------------------------------------

def _demo(client: AzureAppConfigurationClient) -> None:
    # 1. Read one setting.
    setting = get_setting(client, key="myapp/database-url")
    if setting:
        print(f"myapp/database-url = {setting.value!r}")
    else:
        print("myapp/database-url not found.")

    # 2. Read all settings under the "myapp/" prefix in one call.
    settings = get_multiple_settings(client, key_filter="myapp/*")
    print(f"\nFound {len(settings)} setting(s) under 'myapp/*':")
    for s in settings:
        print(f"  [{s.label or '(no label)'}] {s.key} = {s.value!r}")

    # 3. Watch for changes (runs until interrupted).
    print("\nPolling for configuration changes every 15 s. Press Ctrl-C to stop.")
    for changed_sentinel in watch_for_changes(
        client,
        sentinel_key="myapp/sentinel",
        poll_interval=15.0,
    ):
        print(f"Change detected (sentinel etag={changed_sentinel.etag}). Reloading…")
        fresh = get_multiple_settings(client, key_filter="myapp/*")
        for s in fresh:
            print(f"  {s.key} = {s.value!r}")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    )

    conn_str = os.environ.get("AZURE_APPCONFIG_CONNECTION_STRING")
    if not conn_str:
        raise SystemExit(
            "Set AZURE_APPCONFIG_CONNECTION_STRING in your environment.\n"
            "Alternatively, swap the client construction below for:\n"
            "  AzureAppConfigurationClient(endpoint=..., credential=DefaultAzureCredential())"
        )

    with AzureAppConfigurationClient.from_connection_string(conn_str) as client:
        _demo(client)
