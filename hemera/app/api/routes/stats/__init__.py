#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time  2025/2/26 14:01
# @Author  ideal93
# @File  __init__.py.py
# @Brief
from datetime import date, datetime, timedelta
from typing import List, Optional, Tuple

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import desc, func

from hemera.app.api.deps import ReadSessionDep
from hemera.app.api.routes.helper.block import _get_last_block, get_block_count
from hemera.app.api.routes.helper.transaction import (
    GasStats,
    get_gas_stats,
    get_gas_stats_list,
    get_latest_txn_count,
    get_total_txn_count,
    get_transaction_count_stats_list,
)
from hemera.common.models.stats.daily_boards_stats import DailyBoardsStats

router = APIRouter(tags=["STATS"])


class SmartContractMetric(BaseModel):
    contract_address: str = Field(..., description="Smart contract address")
    minute_transaction_count: int = Field(..., description="Transaction count per minute")


class MetricsResponse(BaseModel):
    block_timestamp: datetime = Field(..., description="Timestamp of the block")
    transaction_count_minute: int = Field(..., description="Number of transactions in the current minute")
    transaction_count_total: int = Field(..., description="Total transaction count")
    transaction_per_second: float = Field(..., description="Transactions per second")
    block_per_second: float = Field(..., description="Blocks generated per second")
    gas_stats: GasStats = Field(..., description="Statistics related to gas usage")
    top_active_contracts: List[SmartContractMetric] = Field(
        ..., description="Top active smart contracts based on the previous day's data"
    )


class GasStatsWithBlockTimestamp(BaseModel):
    block_timestamp: datetime = Field(..., description="Timestamp of the block")
    gas_stats: GasStats = Field(..., description="Statistics related to gas usage")


class TransactionCountWithBlockTimestamp(BaseModel):
    block_timestamp: datetime = Field(..., description="Timestamp of the block")
    transaction_count: float = Field(..., description="Number of transactions in the current minute")


class TopActiveContractWithBlockTimestamp(BaseModel):
    block_timestamp: datetime = Field(..., description="Timestamp of the block")
    top_active_contracts: List[SmartContractMetric] = Field(
        ..., description="Top active smart contracts based on the previous day's data"
    )


class TransactionCountListResponse(BaseModel):
    metrics: List[TransactionCountWithBlockTimestamp] = Field(
        ..., description="List of transaction count statistics for the latest period"
    )


class GasStatsListResponse(BaseModel):
    metrics: List[GasStatsWithBlockTimestamp] = Field(..., description="List of metrics for the last N minutes")


class TopActiveContractListResponse(BaseModel):
    metrics: List[TopActiveContractWithBlockTimestamp] = Field(
        ..., description="List of top active contracts for the latest period"
    )


def validate_stats_params(
    duration: timedelta = Query(default=timedelta(minutes=30), description="Duration for stats (e.g., 30 minutes)"),
    interval: timedelta = Query(default=timedelta(minutes=2), description="Time bucket interval (e.g., 2 minutes)"),
    latest_timestamp: Optional[datetime] = Query(
        default=None, description="Optional latest timestamp to override the database's latest timestamp"
    ),
) -> Tuple[timedelta, timedelta, Optional[datetime]]:
    """
    Validates that the given duration is evenly divisible by the interval.
    Returns a tuple of (duration, interval, latest_timestamp) if valid.
    """
    if duration.total_seconds() % interval.total_seconds() != 0:
        raise HTTPException(status_code=400, detail="Duration must be evenly divisible by interval")
    if latest_timestamp is not None:
        latest_timestamp = latest_timestamp.replace(microsecond=0).replace(second=0)
    return duration, interval, latest_timestamp


@router.get("/v1/developer/stats/latest_gas_stats_list", response_model=GasStatsListResponse)
async def get_latest_gas_stats_list(
    session: ReadSessionDep, params: Tuple[timedelta, timedelta, Optional[datetime]] = Depends(validate_stats_params)
):
    duration, interval, latest_timestamp = params
    result = get_gas_stats_list(session, duration, interval, latest_timestamp)
    return GasStatsListResponse(
        metrics=[
            GasStatsWithBlockTimestamp(block_timestamp=block_timestamp, gas_stats=gas_stats)
            for (block_timestamp, gas_stats) in result
        ]
    )


@router.get("/v1/developer/stats/latest_transaction_count_list", response_model=TransactionCountListResponse)
async def get_latest_transaction_count_stats(
    session: ReadSessionDep, params: Tuple[timedelta, timedelta, Optional[datetime]] = Depends(validate_stats_params)
):
    """
    Retrieves transaction count statistics for the latest period specified by duration and interval.
    The duration must be evenly divisible by the interval.
    """
    duration, interval, latest_timestamp = params
    result = get_transaction_count_stats_list(session, duration, interval, latest_timestamp)
    return TransactionCountListResponse(
        metrics=[
            TransactionCountWithBlockTimestamp(block_timestamp=block_timestamp, transaction_count=tx_count_per_second)
            for (block_timestamp, tx_count_per_second) in result
        ]
    )


# TODO
@router.get("/v1/developer/stats/latest_top_active_contracts_list", response_model=TopActiveContractListResponse)
async def get_latest_top_active_contracts(
    session: ReadSessionDep, params: Tuple[timedelta, timedelta, Optional[datetime]] = Depends(validate_stats_params)
):
    duration, interval, latest_timestamp = params

    if latest_timestamp is None:
        latest_timestamp = datetime.utcnow().replace(microsecond=0).replace(second=0)

    start_time = latest_timestamp - duration

    interval_seconds = int(interval.total_seconds())
    total_seconds = int(duration.total_seconds())
    num_buckets = total_seconds // interval_seconds

    buckets = []
    for i in range(num_buckets):
        bucket_time = start_time + timedelta(seconds=i * interval_seconds)

        top_active_contracts = []
        for j in range(20):
            top_active_contracts.append(
                SmartContractMetric(contract_address=f"0x{'{:040x}'.format(j)}", minute_transaction_count=15 + j)
            )

        buckets.append(
            TopActiveContractWithBlockTimestamp(block_timestamp=bucket_time, top_active_contracts=top_active_contracts)
        )

    return TopActiveContractListResponse(metrics=buckets)


@router.get("/v1/developer/stats/metrics", response_model=MetricsResponse)
async def get_address_profile(session: ReadSessionDep):

    block_timestamp = _get_last_block(session, columns="timestamp")
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
        block_timestamp=block_timestamp.replace(microsecond=0).replace(second=0) if block_timestamp else None,
        transaction_count_minute=transaction_count_minute,
        transaction_count_total=transaction_count_total,
        transaction_per_second=transaction_count_minute / 60.0,
        block_per_second=block_times / 60.0,
        gas_stats=gas_stats,
        top_active_contracts=top_active_contracts,
    )


@router.get("/v1/developer/stats/all_boards", response_model=List[str])
async def get_unique_board_ids(session: ReadSessionDep):
    query = session.query(DailyBoardsStats.board_id).distinct()
    result = query.all()
    unique_board_ids = [row[0] for row in result]
    return unique_board_ids


class ECOBoardResponse(BaseModel):
    rank: int
    board_id: str
    block_date: str
    actual_date: str
    key: str
    count: int


class WrappedECOBoardResponse(BaseModel):
    total: int
    page: int
    page_size: int
    list: List[ECOBoardResponse]


time_ranges = {
    "1d": lambda now: now - timedelta(days=1),
    "7d": lambda now: now - timedelta(days=7),
    "30d": lambda now: now - timedelta(days=30),
    "1m": lambda now: now - timedelta(days=30),
    "6m": lambda now: now - timedelta(days=180),
    "YTD": lambda now: datetime(now.year, 1, 1),
    "1y": lambda now: now - timedelta(days=365),
    "all": lambda now: datetime(2020, 1, 1),
}


@router.get("/v1/developer/stats/get_board_data", response_model=WrappedECOBoardResponse)
# get("time_range", "7d")
async def get_board_data(board_id: str, time_range: str, session: ReadSessionDep, page: int = 1, page_size: int = 10):
    today = date.today()
    block_date = time_ranges[time_range](today)
    query = (
        session.query(DailyBoardsStats)
        .filter(DailyBoardsStats.board_id == board_id, DailyBoardsStats.block_date == block_date)
        .order_by(desc(DailyBoardsStats.count))
    )

    total_count = query.count()
    result = query.offset((page - 1) * page_size).limit(page_size).all()
    if not result:
        closest_block_date = (
            session.query(DailyBoardsStats.block_date)
            .filter(DailyBoardsStats.board_id == board_id)
            .order_by(
                func.abs(func.date_part("epoch", DailyBoardsStats.block_date) - func.date_part("epoch", block_date))
            )
            .limit(1)
            .scalar()
        )
        if not closest_block_date:
            return []
        query = (
            session.query(DailyBoardsStats)
            .filter(DailyBoardsStats.board_id == board_id, DailyBoardsStats.block_date == closest_block_date)
            .order_by(desc(DailyBoardsStats.count))
        )

        total_count = query.count()
        result = query.offset((page - 1) * page_size).limit(page_size).all()

    data = [
        ECOBoardResponse(
            rank=(page - 1) * page_size + index + 1,  # 计算全局排名
            board_id=row.board_id,
            block_date=block_date.strftime("%Y-%m-%d"),
            actual_date=row.block_date.strftime("%Y-%m-%d"),
            key=row.key,
            count=row.count,
        )
        for index, row in enumerate(result)
    ]

    return WrappedECOBoardResponse(total=total_count, page=page, page_size=page_size, list=data)
