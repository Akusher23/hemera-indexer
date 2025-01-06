#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import threading
from dataclasses import dataclass
from typing import Dict

from prometheus_client import REGISTRY, Counter, Gauge, start_http_server
from prometheus_client.metrics_core import GaugeMetricFamily

METRICS_CLIENT_PORT = int(os.environ.get("METRICS_CLIENT_PORT", "9200"))


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


class MetricStore:
    def __init__(self):
        # Gauge metrics
        self.indexed_ranges: Dict[str, float] = {}  # block_range -> value
        self.exported_ranges: Dict[tuple, float] = {}  # (block_range, status) -> value
        self.total_processing_durations: Dict[str, float] = {}  # block_range -> duration
        self.job_processing_durations: Dict[tuple, float] = {}  # (block_range, sub_job_name) -> duration
        self.export_processing_durations: Dict[tuple, float] = {}  # (block_range, domains) -> duration
        self.retry_counts: Dict[tuple, int] = {}  # (block_range, sub_job_name) -> retry_count


class MetricsCollector:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, job_name: str = None):
        if hasattr(self, "_initialized"):
            return
        start_http_server(METRICS_CLIENT_PORT)

        self.job_name = job_name if job_name else "default"

        # Metrics that need to be cleaned after storage pull
        self.store_lock = threading.Lock()
        self.store = MetricStore()

        # Metrics that require no additional memory management
        self._metrics_definition()

        REGISTRY.register(self)
        self._initialized = True

    def collect(self):
        with self.store_lock:
            metrics = self._get_metrics_snapshot()
            self._clear_store_metrics()

        for name, help_text, label_names, value_list in metrics:
            metric = GaugeMetricFamily(name, help_text, labels=label_names)
            for labels, value in value_list:
                metric.add_metric([self.job_name] + labels, value)
            yield metric

    def _get_metrics_snapshot(self):
        return [
            (
                "indexed_range",
                "Current indexed blocks between range",
                ["job_name", "block_range"],
                [([block_range], value) for block_range, value in self.store.indexed_ranges.items()],
            ),
            (
                "exported_range",
                "Current exported blocks between range",
                ["job_name", "block_range", "status"],
                [([block_range, status], value) for (block_range, status), value in self.store.exported_ranges.items()],
            ),
            (
                "total_processing_duration",
                "Total time spent processing each block range in milliseconds",
                ["job_name", "block_range"],
                [([block_range], value) for block_range, value in self.store.total_processing_durations.items()],
            ),
            (
                "job_processing_duration",
                "Time spent in each sub-job processing block range in milliseconds",
                ["job_name", "block_range", "sub_job_name"],
                [
                    ([block_range, job_name], value)
                    for (block_range, job_name), value in self.store.job_processing_durations.items()
                ],
            ),
            (
                "export_domains_processing_duration",
                "Time spent in each exporting domain between block range in milliseconds",
                ["job_name", "block_range", "domains"],
                [
                    ([block_range, domains], value)
                    for (block_range, domains), value in self.store.export_processing_durations.items()
                ],
            ),
            (
                "job_processing_retry",
                "Retry times in sub-job processing block range",
                ["job_name", "block_range", "sub_job_name"],
                [
                    ([block_range, job_name], value)
                    for (block_range, job_name), value in self.store.retry_counts.items()
                ],
            ),
        ]

    def _clear_store_metrics(self):
        self.store.indexed_ranges.clear()
        self.store.exported_ranges.clear()
        self.store.total_processing_durations.clear()
        self.store.job_processing_durations.clear()
        self.store.export_processing_durations.clear()
        self.store.retry_counts.clear()

    def _metrics_definition(self):
        self.last_sync_record = Gauge("last_sync_record", "The last synced block number", ["job_name"])

        self.indexed_domains = Counter(
            "indexed_domains", "Total number of indexed domains", ["job_name", "domain", "status"]
        )

        self.exported_domains = Counter(
            "exported_domains", "Total number of exported domains", ["job_name", "domain", "status"]
        )

    def update_last_sync_record(self, last_sync_record: int):
        last_record = self.last_sync_record.labels(job_name=self.job_name)._value.get()
        if last_record < last_sync_record:
            self.last_sync_record.labels(job_name=self.job_name).set(last_sync_record)

    def update_indexed_range(self, index_range: str):
        with self.store_lock:
            self.store.indexed_ranges[index_range] = 1

    def update_exported_range(self, index_range: str, status: str):
        with self.store_lock:
            self.store.exported_ranges[(index_range, status)] = 1

    def update_indexed_domains(self, domain: str, status: str, amount: int):
        self.indexed_domains.labels(job_name=self.job_name, domain=domain, status=status).inc(amount)

    def update_exported_domains(self, domain: str, status: str, amount: int):
        self.exported_domains.labels(job_name=self.job_name, domain=domain, status=status).inc(amount)

    def update_total_processing_duration(self, block_range: str, duration: int):
        with self.store_lock:
            self.store.total_processing_durations[block_range] = duration

    def update_job_processing_duration(self, block_range: str, job_name: str, duration: int):
        with self.store_lock:
            self.store.job_processing_durations[(block_range, job_name)] = duration

    def update_export_domains_processing_duration(self, block_range: str, domains: str, duration: int):
        with self.store_lock:
            self.store.export_processing_durations[(block_range, domains)] = duration

    def update_job_processing_retry(self, block_range: str, job_name: str, retry: int):
        with self.store_lock:
            self.store.retry_counts[(block_range, job_name)] = retry
