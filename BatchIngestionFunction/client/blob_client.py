# client/blob_client.py
from urllib.parse import urlparse
import io
import logging
import os

from azure.storage.blob import BlobServiceClient

# READ ENV once
_STORAGE_CONN = os.environ.get("STORAGE_CONNECTION_STRING")
if not _STORAGE_CONN:
    raise RuntimeError("Missing STORAGE_CONNECTION_STRING env var for blob client")

_blob_service_client = BlobServiceClient.from_connection_string(_STORAGE_CONN)


def read_blob_text(file_url: str, encoding: str = "utf-8") -> str:
    """
    Download blob content as text.
    Uses content_as_text() when available; falls back to readall() and decode.
    """
    if not file_url:
        raise ValueError("file_url is required")

    parsed = urlparse(file_url)
    # parsed.path looks like "/<container>/<path/to/blob>"
    path = parsed.path.lstrip("/")
    parts = path.split("/", 1)
    if len(parts) == 0:
        raise ValueError(f"Invalid file_url: {file_url}")
    container_name = parts[0]
    blob_path = parts[1] if len(parts) > 1 else ""

    try:
        container_client = _blob_service_client.get_container_client(container_name)
        blob_client = container_client.get_blob_client(blob_path)
        downloader = blob_client.download_blob()
        try:
            return downloader.content_as_text(encoding=encoding)
        except Exception:
            # fallback (older SDKs / streaming issues)
            raw = downloader.readall()
            return raw.decode(encoding)
    except Exception as e:
        logging.error(f"Failed to read blob {file_url}: {e}")
        raise
