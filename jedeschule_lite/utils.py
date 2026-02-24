"""Shared utilities for jedeschule_lite scrapers."""

import time
import requests

DEFAULT_TIMEOUT = 30
MAX_RETRIES = 3


def fetch(url: str, *, timeout: int = DEFAULT_TIMEOUT, **kwargs) -> requests.Response:
    """Fetch a URL with retry logic."""
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(url, timeout=timeout, **kwargs)
            response.raise_for_status()
            return response
        except requests.RequestException as e:
            if attempt < MAX_RETRIES - 1:
                wait = 2 ** (attempt + 1)
                print(f"  Retry {attempt + 1}/{MAX_RETRIES} for {url[:80]}...: {e}")
                time.sleep(wait)
            else:
                raise


def post(url: str, *, timeout: int = DEFAULT_TIMEOUT, **kwargs) -> requests.Response:
    """POST to a URL with retry logic."""
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.post(url, timeout=timeout, **kwargs)
            response.raise_for_status()
            return response
        except requests.RequestException as e:
            if attempt < MAX_RETRIES - 1:
                wait = 2 ** (attempt + 1)
                print(f"  Retry {attempt + 1}/{MAX_RETRIES} for {url[:80]}...: {e}")
                time.sleep(wait)
            else:
                raise


def cleanjoin(items: list, sep: str = "") -> str:
    """Strip whitespace and join items."""
    return sep.join(text.strip() for text in items).strip()


def get_first_or_none(items: list):
    """Return first item or None."""
    return items[0] if items else None


def safe_strip(value: str | None) -> str | None:
    """Strip whitespace, return None if empty."""
    if not value or not value.strip():
        return None
    return value.strip()


def parse_geojson_features(data: dict) -> list[dict]:
    """Parse GeoJSON and yield feature dicts with lon/lat."""
    results = []
    for feature in data.get("features", []):
        props = feature.get("properties", {})
        coords = feature.get("geometry", {}).get("coordinates", [])
        if len(coords) >= 2:
            props["lon"] = coords[0]
            props["lat"] = coords[1]
        results.append(props)
    return results
