import os
import shutil
import tempfile
from pathlib import Path

import pytest

from traider.platforms.cache import get_shared_cache
from traider.platforms.cache.disk_cache_backend import DiskCacheBackend


@pytest.fixture()
def tmp_cache_dir(tmp_path):
    """Create an isolated directory for cache tests."""
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    yield cache_dir
    # Cleanup – diskcache uses SQLite + shards; ensure directory removal
    shutil.rmtree(cache_dir, ignore_errors=True)


def test_basic_set_get_and_len(tmp_cache_dir):
    cache = DiskCacheBackend(directory=tmp_cache_dir, max_items=10)
    cache["foo"] = 1
    cache["bar"] = 2

    assert cache["foo"] == 1
    assert cache["bar"] == 2
    assert len(cache) == 2


def test_add_helper(tmp_cache_dir):
    cache = DiskCacheBackend(directory=tmp_cache_dir, max_items=10)
    assert cache.add("id1") is True  # first time
    assert cache.add("id1") is False  # duplicate
    assert len(cache) == 1


def test_lru_eviction(tmp_cache_dir):
    max_items = 3
    cache = DiskCacheBackend(directory=tmp_cache_dir, max_items=max_items)
    # Insert max_items + 1 keys
    for i in range(max_items + 1):
        cache[str(i)] = i
    # Oldest key "0" must be evicted
    assert len(cache) == max_items
    assert "0" not in cache


def test_persistence_across_instances(tmp_cache_dir):
    key, value = "persist", "ok"
    cache1 = DiskCacheBackend(directory=tmp_cache_dir, max_items=10)
    cache1[key] = value
    cache1.close()

    # Re-open
    cache2 = DiskCacheBackend(directory=tmp_cache_dir, max_items=10)
    assert cache2[key] == value


def test_in_memory_fallback(monkeypatch):
    """TRAIDER_NO_CACHE triggers in-memory cache."""
    monkeypatch.setenv("TRAIDER_NO_CACHE", "1")
    # Force refresh of shared cache singleton
    from importlib import reload
    import traider.platforms.cache as cache_mod  # noqa: E402

    reload(cache_mod)
    cache = cache_mod.get_shared_cache()
    cache.clear()

    assert cache.add("abc") is True
    assert cache.add("abc") is False
    cache.close()
