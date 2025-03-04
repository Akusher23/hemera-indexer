#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time  2025/3/4 14:35
# @Author  ideal93
# @File  transaction_trace_json.py.py
# @Brief

from dataclasses import dataclass
from typing import Any

from hemera.indexer.domains import Domain


@dataclass
class TransactionTraceJson(Domain):
    transaction_hash: str
    block_timestamp: int
    block_number: int
    block_hash: str
    data: dict[str, Any]

    @staticmethod
    def from_rpc(trace_dict: dict):
        return TransactionTraceJson(
            block_number=trace_dict["block_number"],
            block_hash=trace_dict["block_hash"],
            block_timestamp=trace_dict["block_timestamp"],
            transaction_hash=trace_dict["txHash"],
            data=trace_dict,
        )
