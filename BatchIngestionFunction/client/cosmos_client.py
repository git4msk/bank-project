# client/cosmos_client.py
import os
import logging
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Iterable, Dict, Any, Tuple

from azure.cosmos import CosmosClient, PartitionKey
from azure.cosmos.exceptions import CosmosHttpResponseError

# Env & defaults
_COSMOS_CONN = os.environ.get("COSMOS_DB_CONNECTION_STRING")
_COSMOS_DB = os.environ.get("COSMOS_DB_NAME", "operation-storage-db")
_UPSERT_WORKERS = int(os.environ.get("UPSERT_WORKERS", "8"))
_UPSERT_RETRIES = int(os.environ.get("UPSERT_RETRIES", "3"))
_UPSERT_RETRY_BACKOFF = float(os.environ.get("UPSERT_RETRY_BACKOFF", "0.5"))

if not _COSMOS_CONN:
    raise RuntimeError("Missing COSMOS_DB_CONNECTION_STRING env var for cosmos client")

_client = CosmosClient.from_connection_string(_COSMOS_CONN)
_database = _client.create_database_if_not_exists(id=_COSMOS_DB, offer_throughput=400)


# -------------------------
# container creation (Option A)
# -------------------------
def get_or_create_container(container_id: str, partition_key_path: str):
    """
    Create container if not exists and return container client.
    partition_key_path should include leading slash, e.g. "/AccountNumber"
    """
    try:
        container = _database.create_container_if_not_exists(
            id=container_id,
            partition_key=PartitionKey(path=partition_key_path)
        )
        return container
    except Exception as e:
        logging.error(f"Failed to create/get cosmos container {container_id}: {e}")
        raise


# -------------------------
# Basic sanitizer: datetime -> ISO recursively
# -------------------------
def _sanitize_for_cosmos(obj):
    """
    Replace datetime objects (recursively) with ISO strings.
    Returns the same object mutated where possible.
    """
    from datetime import datetime as _dt

    if isinstance(obj, _dt):
        return obj.isoformat()
    if isinstance(obj, dict):
        return {k: _sanitize_for_cosmos(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize_for_cosmos(x) for x in obj]
    return obj


# -------------------------
# Upsert helpers with retries & parallelism (v4 SDK friendly)
# -------------------------
def _retry_op(fn, *args, **kwargs):
    last_exc = None
    for attempt in range(1, _UPSERT_RETRIES + 1):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            last_exc = e
            sleep = _UPSERT_RETRY_BACKOFF * (2 ** (attempt - 1))
            logging.warning(f"Retry {attempt}/{_UPSERT_RETRIES} for {fn.__name__}: {e} (sleep {sleep}s)")
            time.sleep(sleep)
    logging.error(f"Operation {fn.__name__} failed after {_UPSERT_RETRIES} attempts: {last_exc}")
    raise last_exc


def upsert_item(container, item: Dict[str, Any]) -> None:
    """
    Upsert single item with sanitization and basic retry.
    Container here is the Cosmos container object returned from get_or_create_container.
    """
    sanitized = _sanitize_for_cosmos(item)
    _retry_op(container.upsert_item, sanitized)


def upsert_items_parallel(container, items: Iterable[Dict[str, Any]], workers: int = None) -> Tuple[int, int]:
    """
    Upsert many items in parallel using ThreadPoolExecutor.
    Returns (success_count, fail_count).
    Each item is sanitized before upsert.
    """
    workers = workers or _UPSERT_WORKERS
    successes = 0
    failures = 0
    futures = []

    def _worker(itm):
        sanitized = _sanitize_for_cosmos(itm)
        _retry_op(container.upsert_item, sanitized)

    with ThreadPoolExecutor(max_workers=workers) as exe:
        for itm in items:
            # ensure an 'id' exists (Cosmos requirement)
            if not itm.get("id"):
                itm["id"] = str(datetime.utcnow().timestamp())
            futures.append(exe.submit(_worker, itm))

        for f in as_completed(futures):
            ex = f.exception()
            if ex:
                logging.error(f"Item upsert failed: {ex}")
                failures += 1
            else:
                successes += 1

    return successes, failures
