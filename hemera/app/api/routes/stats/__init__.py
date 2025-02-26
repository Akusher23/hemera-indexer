#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time  2025/2/26 14:01
# @Author  ideal93
# @File  __init__.py.py
# @Brief
from datetime import timedelta
from typing import List

from fastapi import APIRouter
from pydantic import BaseModel, Field

from hemera.app.api.deps import ReadSessionDep
from hemera.app.api.routes.helper.block import get_block_count
from hemera.app.api.routes.helper.transaction import GasStats, get_gas_stats, get_latest_txn_count, get_total_txn_count

router = APIRouter(tags=["STATS"])


class SmartContractMetric(BaseModel):
    contract_address: str = Field(..., description="Smart contract address")
    minute_transaction_count: int = Field(..., description="Transaction count per minute")


class MetricsResponse(BaseModel):
    transaction_count_minute: int = Field(..., description="Number of transactions in the current minute")
    transaction_count_total: int = Field(..., description="Total transaction count")
    transaction_per_second: float = Field(..., description="Transactions per second")
    block_per_second: int = Field(..., description="Blocks generated per second")
    gas_stats: GasStats = Field(..., description="Statistics related to gas usage")
    top_active_contracts: List[SmartContractMetric] = Field(
        ..., description="Top active smart contracts based on the previous day's data"
    )


@router.get("/v1/developer/stats/metrics", response_model=MetricsResponse)
async def get_address_profile(session: ReadSessionDep):
    transaction_count_minute = get_latest_txn_count(session, timedelta(minutes=1))
    transaction_count_total = get_total_txn_count(session)
    block_times = get_block_count(session, timedelta(minutes=1))
    gas_stats = get_gas_stats(session, timedelta(minutes=1))

    top_active_contracts = []
    for i in range(20):
        top_active_contracts.append(
            SmartContractMetric(contract_address=f"0x{'{:040x}'.format(i)}", minute_transaction_count=15 + i)
        )

    return MetricsResponse(
        transaction_count_minute=transaction_count_minute,
        transaction_count_total=transaction_count_total,
        transaction_per_second=transaction_count_minute / 60.0,
        block_per_second=block_times / 60.0,
        gas_stats=gas_stats,
        top_active_contracts=top_active_contracts,
    )
