#!/usr/bin/env python
# -*- coding: utf-8 -*-
import heapq
import os
from dataclasses import dataclass
from typing import List

from prometheus_client import Counter, Gauge, Histogram, start_http_server

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

    def __contains__(self, block_range: str):
        return block_range in self.range_set


class MetricsCollector:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, job_name: str = None, port: int = 9200):
        if hasattr(self, "_initialized"):
            return
        start_http_server(port)

        self.job_name = job_name if job_name else "default"
        self.active_ranges = RangeHeap(METRICS_KEEP_RANGE)

        self._metrics_definition()
        self._initialized = True

    def _metrics_definition(self):
        self.last_sync_record = Gauge("last_sync_record", "The last synced block number", ["job_name"])

        self.indexed_range = Gauge("indexed_range", "Current indexed blocks between range", ["job_name", "block_range"])

        self.exported_range = Gauge(
            "exported_range", "Current exported blocks between range", ["job_name", "block_range", "status"]
        )

        self.indexed_domains = Counter(
            "indexed_domains", "Total number of indexed domains", ["job_name", "domain", "status"]
        )

        self.exported_domains = Counter(
            "exported_domains", "Total number of exported domains", ["job_name", "domain", "status"]
        )

        self.total_processing_duration = Gauge(
            "total_processing_duration",
            "Total time spent processing each block range in milliseconds",
            ["job_name", "block_range"],
        )

        self.job_processing_duration = Gauge(
            "job_processing_duration",
            "Time spent in each sub-job processing block range in milliseconds",
            ["job_name", "block_range", "sub_job_name"],
        )

        self.export_domains_processing_duration = Gauge(
            "export_domains_processing_duration",
            "Time spent in each sub-job processing block range in milliseconds",
            ["job_name", "block_range", "domains"],
        )

        self.job_processing_retry = Gauge(
            "job_processing_retry",
            "Retry times in sub-job processing block range",
            ["job_name", "block_range", "sub_job_name"],
        )

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
            if labels[0] == self.job_name and labels[1] == indexed_range:
                self.indexed_range.remove(*labels)

        existing_metrics = self.exported_range._metrics.copy()
        for labels in existing_metrics:
            if labels[0] == self.job_name and labels[1] == indexed_range:
                self.exported_range.remove(*labels)

        existing_metrics = self.total_processing_duration._metrics.copy()
        for labels in existing_metrics:
            if labels[0] == self.job_name and labels[1] == indexed_range:
                self.total_processing_duration.remove(*labels)

        existing_metrics = self.job_processing_duration._metrics.copy()
        for labels in existing_metrics:
            if labels[0] == self.job_name and labels[1] == indexed_range:
                self.job_processing_duration.remove(*labels)

        existing_metrics = self.export_domains_processing_duration._metrics.copy()
        for labels in existing_metrics:
            if labels[0] == self.job_name and labels[1] == indexed_range:
                self.export_domains_processing_duration.remove(*labels)

        existing_metrics = self.job_processing_retry._metrics.copy()
        for labels in existing_metrics:
            if labels[0] == self.job_name and labels[1] == indexed_range:
                self.job_processing_retry.remove(*labels)

    def get_active_ranges(self) -> List[str]:
        return self.active_ranges.get_ranges()

    def update_last_sync_record(self, last_sync_record: int):
        last_record = self.last_sync_record.labels(job_name=self.job_name)._value.get()
        if last_record < last_sync_record:
            self.last_sync_record.labels(job_name=self.job_name).set(last_sync_record)

    def update_indexed_range(self, index_range: str):
        if index_range not in self.active_ranges:
            self._update_active_range(index_range)
            self.indexed_range.labels(job_name=self.job_name, block_range=index_range).set(1)

    def update_exported_range(self, index_range: str, status: str):
        self.exported_range.labels(job_name=self.job_name, block_range=index_range, status=status).set(1)

    def update_indexed_domains(self, domain: str, status: str, amount: int):
        self.indexed_domains.labels(job_name=self.job_name, domain=domain, status=status).inc(amount)

    def update_exported_domains(self, domain: str, status: str, amount: int):
        self.exported_domains.labels(job_name=self.job_name, domain=domain, status=status).inc(amount)

    def update_total_processing_duration(self, block_range: str, duration: int):
        self.total_processing_duration.labels(job_name=self.job_name, block_range=block_range).set(duration)

    def update_job_processing_duration(self, block_range: str, job_name: str, duration: int):
        self.job_processing_duration.labels(job_name=self.job_name, block_range=block_range, sub_job_name=job_name).set(
            duration
        )

    def update_export_domains_processing_duration(self, block_range: str, domains: str, duration: int):
        self.export_domains_processing_duration.labels(
            job_name=self.job_name, block_range=block_range, domains=domains
        ).set(duration)

    def update_job_processing_retry(self, block_range: str, job_name: str, retry: int):
        self.job_processing_duration.labels(job_name=self.job_name, block_range=block_range, sub_job_name=job_name).set(
            retry
        )
