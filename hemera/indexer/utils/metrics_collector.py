#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os

from prometheus_client import Counter, Gauge, start_http_server

METRICS_CLIENT_PORT = int(os.environ.get("METRICS_CLIENT_PORT", "9200"))


class MetricsCollector:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, instance_name: str = None):
        if hasattr(self, "_initialized"):
            return
        start_http_server(METRICS_CLIENT_PORT)

        self.instance_name = instance_name if instance_name else "default"

        # Metrics that require no additional memory management
        self._metrics_definition()

        self._initialized = True

    def _metrics_definition(self):
        self.last_sync_record = Gauge("last_sync_record", "The last synced block number", ["instance"])

        self.failure_batch_counter = Counter("failure_batch_counter", "Total number of failed index", ["instance"])

        self.indexed_domains = Counter(
            "indexed_domains", "Total number of indexed domains", ["instance", "domain", "status"]
        )

        self.exported_domains = Counter(
            "exported_domains", "Total number of exported domains", ["instance", "domain", "status"]
        )

        self.total_processing_duration = Gauge(
            "total_processing_duration",
            "Total time spent processing each block range in milliseconds",
            ["instance"],
        )

        self.job_processing_duration = Gauge(
            "job_processing_duration",
            "Time spent in each sub-job processing block range in milliseconds",
            ["instance", "job_name"],
        )

        self.export_domains_processing_duration = Gauge(
            "export_domains_processing_duration",
            "Time spent in each sub-job processing block range in milliseconds",
            ["instance", "domains"],
        )

        self.job_processing_retry = Counter(
            "job_processing_retry",
            "Retry times in sub-job processing block range",
            ["instance", "job_name"],
        )

    def update_last_sync_record(self, last_sync_record: int):
        last_record = self.last_sync_record.labels(instance=self.instance_name)._value.get()
        if last_record < last_sync_record:
            self.last_sync_record.labels(instance=self.instance_name).set(last_sync_record)

    def update_failure_batch_counter(self):
        self.failure_batch_counter.labels(instance=self.instance_name).inc(1)

    def update_indexed_domains(self, domain: str, status: str, amount: int):
        self.indexed_domains.labels(instance=self.instance_name, domain=domain, status=status).inc(amount)

    def update_exported_domains(self, domain: str, status: str, amount: int):
        self.exported_domains.labels(instance=self.instance_name, domain=domain, status=status).inc(amount)

    def update_total_processing_duration(self, duration: int):
        self.total_processing_duration.labels(instance=self.instance_name).set(duration)

    def update_job_processing_duration(self, job_name: str, duration: int):
        self.job_processing_duration.labels(instance=self.instance_name, job_name=job_name).set(duration)

    def update_export_domains_processing_duration(self, domains: str, duration: int):
        self.export_domains_processing_duration.labels(instance=self.instance_name, domains=domains).set(duration)

    def update_job_processing_retry(self, job_name: str, retry: int):
        self.job_processing_retry.labels(instance=self.instance_name, job_name=job_name).inc(retry)
