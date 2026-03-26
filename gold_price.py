"""
Real-time gold price fetcher with caching.
Tries free APIs first, falls back to env var.
"""
import os
import time
from datetime import datetime

import requests

# Cache: (price_per_gram_24k, timestamp, source)
_cache = {"price": None, "timestamp": 0, "source": None}
CACHE_TTL = 300  # 5 minutes


def _fetch_goldprice_org() -> float | None:
    """Try goldprice.org free endpoint. Returns EUR per gram of 24k gold."""
    try:
        resp = requests.get(
            "https://data-asg.goldprice.org/dbXRates/EUR",
            timeout=10,
            headers={"User-Agent": "Mozilla/5.0"},
        )
        if resp.status_code == 200:
            data = resp.json()
            # Response has items list with xauPrice (per troy oz)
            items = data.get("items", [])
            if items:
                price_per_oz = items[0].get("xauPrice", 0)
                if price_per_oz:
                    # 1 troy oz = 31.1035 grams
                    return price_per_oz / 31.1035
    except Exception as e:
        print(f"goldprice.org failed: {e}")
    return None


def _fetch_swissquote() -> float | None:
    """Try Swissquote forex feed. Returns EUR per gram of 24k gold."""
    try:
        resp = requests.get(
            "https://forex-data-feed.swissquote.com/public-quotes/bboquotes/instrument/XAU/EUR",
            timeout=10,
        )
        if resp.status_code == 200:
            data = resp.json()
            if data and isinstance(data, list):
                spread = data[0].get("spreadProfilePrices", [{}])
                if spread:
                    bid = spread[0].get("bid", 0)
                    ask = spread[0].get("ask", 0)
                    price_per_oz = (bid + ask) / 2 if bid and ask else bid or ask
                    if price_per_oz:
                        return price_per_oz / 31.1035
    except Exception as e:
        print(f"Swissquote failed: {e}")
    return None


def _fetch_from_env() -> float | None:
    """Fallback: read from environment variable."""
    val = os.getenv("GOLD_PRICE_PER_GRAM")
    if val:
        try:
            return float(val)
        except ValueError:
            pass
    return None


def get_current_gold_price() -> float:
    """
    Get current gold price per gram in EUR (24k pure gold).
    Caches result for 5 minutes. Tries multiple sources.
    Returns float or falls back to 92.0 as last resort.
    """
    global _cache
    now = time.time()

    # Return cached value if fresh
    if _cache["price"] is not None and (now - _cache["timestamp"]) < CACHE_TTL:
        return _cache["price"]

    # Try sources in order
    price = _fetch_goldprice_org()
    source = "goldprice.org"

    if price is None:
        price = _fetch_swissquote()
        source = "swissquote.com"

    if price is None:
        price = _fetch_from_env()
        source = "manual"

    if price is None:
        price = 92.0
        source = "default"

    _cache = {"price": price, "timestamp": now, "source": source}
    return price


def get_18k_gold_price() -> float:
    """
    Get price per gram of 18k gold (75% pure).
    18k = 75% of 24k price.
    """
    return get_current_gold_price() * 0.75


def get_gold_info() -> dict:
    """Return a dict with all gold price information."""
    price_24k = get_current_gold_price()
    return {
        "price_24k_gram": round(price_24k, 2),
        "price_18k_gram": round(price_24k * 0.75, 2),
        "last_updated": datetime.now().isoformat(),
        "source": _cache.get("source", "unknown"),
    }
