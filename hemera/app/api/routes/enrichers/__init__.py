#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time  2025/1/3 16:49
# @Author  ideal93
# @File  __init__.py.py
# @Brief

from abc import abstractmethod
from datetime import datetime, timedelta
from typing import Any, Dict, Generic, List, Set, TypeVar

from sqlmodel import Session

K = TypeVar("K")
V = TypeVar("V")
T = TypeVar("T")


class BaseMapper(Generic[K, V]):
    """Base mapper for data enrichment with cache"""

    def __init__(self, session: Session, refresh_interval: int = 3600):
        self.session = session
        self._cache: Dict[K, V] = {}
        self._last_refresh: Dict[K, datetime] = {}
        self.refresh_interval = timedelta(seconds=refresh_interval)

    @abstractmethod
    def fetch_data(self, keys: Set[K]) -> Dict[K, V]:
        """Fetch data from database"""
        pass

    def get_mapping(self, keys: Set[K], force_refresh: bool = False) -> Dict[K, V]:
        """Get mapping with cache support"""
        now = datetime.now()
        result = {}
        missing_keys = set()

        # Check cache and expiration
        for key in keys:
            if (
                not force_refresh
                and key in self._cache
                and now - self._last_refresh.get(key, datetime.min) < self.refresh_interval
            ):
                result[key] = self._cache[key]
            else:
                missing_keys.add(key)

        # Fetch missing or expired data
        if missing_keys:
            new_data = self.fetch_data(missing_keys)
            for k, v in new_data.items():
                self._cache[k] = v
                self._last_refresh[k] = now
                result[k] = v

        return result

    def clear_cache(self):
        """Clear all cache data"""
        self._cache.clear()
        self._last_refresh.clear()


class BaseFormatter(Generic[T, V]):
    """Base formatter for data conversion"""

    @abstractmethod
    def format(self, item: T, context: Dict[str, Any]) -> V:
        """Format single item with context"""
        pass

    def format_batch(self, items: List[T], context: Dict[str, Any]) -> List[V]:
        """Format multiple items with same context"""
        return [self.format(item, context) for item in items]
