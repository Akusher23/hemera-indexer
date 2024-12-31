#!/usr/bin/env python
# -*- coding: utf-8 -*-
import heapq
import os
from collections import defaultdict
from dataclasses import dataclass
from typing import List

from prometheus_client import Counter, Gauge, start_http_server

METRICS_KEEP_RANGE = int(os.environ.get("METRICS_KEEP_RANGE", "10"))


@dataclass(frozen=True)
class BlockRange:
    start: int
    end: int
    range_str: str

    def __lt__(self, other):
        return self.start < other.start

    @classmethod
    def from_str(cls, range_str: str):
        try:
            start, end = map(int, range_str.split("-"))
            return cls(start, end, range_str)
        except (ValueError, AttributeError):
            raise ValueError(f"Invalid block range format: {range_str}")


class RangeHeap:

    def __init__(self, maxsize: int):
        self.maxsize = maxsize
        self.heap = []
        self.range_set = set()

    def add(self, block_range: BlockRange):
        if block_range.range_str in self.range_set:
            return None

        self.range_set.add(block_range.range_str)
        heapq.heappush(self.heap, block_range)

        if len(self.heap) > self.maxsize:
            oldest = heapq.heappop(self.heap)
            self.range_set.remove(oldest.range_str)
            return oldest
        return None

    def get_ranges(self) -> List[str]:
        return [r.range_str for r in sorted(self.heap)]

    def __len__(self):
        return len(self.heap)


class MetricsCollector:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, port: int = 9200):
        if hasattr(self, "_initialized"):
            return
        start_http_server(port)

        self.active_domains = defaultdict(set)
        self.active_ranges = RangeHeap(METRICS_KEEP_RANGE)

        self._metrics_definition()
        self._initialized = True

    def _metrics_definition(self):
        self.indexed_range = Gauge(
            "indexed_range", "Current processed blocks between range", ["indexed_range", "status"]
        )

        self.indexed_domains = Gauge(
            "indexed_domains", "Current processed domains between range", ["indexed_range", "domain"]
        )

        self.indexed_counter = Counter("indexed_counter", "Total number of indexed blocks", ["status"])

    @staticmethod
    def _parse_range(block_range: str):
        try:
            start, end = map(int, block_range.split("-"))
            return start, end
        except ValueError:
            raise ValueError(f"Invalid block range format: {block_range}")

    def _update_active_range(self, indexed_range: str):
        try:
            range_obj = BlockRange.from_str(indexed_range)
        except ValueError as e:
            print(f"Warning: {e}")
            return

        removed_range = self.active_ranges.add(range_obj)
        if removed_range:
            self._cleanup_range_metrics(removed_range.range_str)

    def _cleanup_range_metrics(self, indexed_range: str):
        existing_metrics = self.indexed_range._metrics.copy()
        for labels in existing_metrics:
            if labels[0] == indexed_range:
                self.indexed_range.remove(*labels)

        existing_domains = self.indexed_domains._metrics.copy()
        for labels in existing_domains:
            if labels[0] == indexed_range:
                self.indexed_domains.remove(*labels)

        self.active_domains.pop(indexed_range, None)

    def get_active_ranges(self) -> List[str]:
        return self.active_ranges.get_ranges()

    def update_indexed_range(self, indexed_range: str, status: str):
        self._update_active_range(indexed_range)
        self.indexed_range.labels(indexed_range=indexed_range, status=status).set(1)

    def update_domains_counter(self, domain: str, indexed_range: str, amount: int):
        self._update_active_range(indexed_range)

        self.active_domains[indexed_range].add(domain)
        self.indexed_domains.labels(indexed_range=indexed_range, domain=domain).set(amount)

    def update_indexed_counter(self, status: str, amount: int):
        self.indexed_counter.labels(
            status=status,
        ).inc(amount)


if __name__ == "__main__":
    metrics = MetricsCollector(keep_ranges=3)

    # 添加测试数据（顺序随机）
    test_ranges = [
        "2000-2999",
        "1000-1999",
        "4000-4999",
        "3000-3999",
        "5000-5999",
    ]

    for block_range in test_ranges:
        metrics.update_indexed_range(block_range, "success")
        metrics.update_domains_counter("example.com", block_range, 42)

        # 打印当前活跃的区间（始终有序）
        print(f"Active ranges: {metrics.get_active_ranges()}")
