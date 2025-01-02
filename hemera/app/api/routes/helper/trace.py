#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time  2025/1/2 15:18
# @Author  ideal93
# @File  trace.py
# @Brief

from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any, List, Optional, Union

from pydantic import BaseModel
from sqlmodel import Session, and_, desc, func, or_, select
from typing_extensions import Literal

from hemera.app.api.routes.helper import ColumnType, process_columns
from hemera.app.api.routes.helper.address import get_txn_cnt_by_address
from hemera.common.enumeration.txn_type import AddressTransactionType
from hemera.common.models.scheduled_metadata import ScheduledMetadata
from hemera.common.models.transactions import Transactions
from hemera.common.utils.format_utils import bytes_to_hex_str, hex_str_to_bytes
from hemera_udf.address_index.models.address_transactions import AddressTransactions
from hemera_udf.stats.models.daily_transactions_stats import DailyTransactionsStats


def get_traces_by_hash(
    session: Session, transaction_hash: Union[str, bytes], columns: ColumnType = "*"
) -> List[InternalTransactionResponse]:
    """Get internal transactions by transaction hash

    Args:
        session: SQLModel session
        transaction_hash: Transaction hash in hex string format
        columns: Can be "*" for all columns, single column name, or list of column names
                Examples:
                - "*": select all columns
                - "block_number": select only block number
                - "block_number,hash": select block number and hash columns
                - ["block_number", "hash"]: select block number and hash columns

    Returns:
        List[InternalTransactionResponse]: List of standardized transaction responses
    """
    if isinstance(transaction_hash, str):
        transaction_hash = hex_str_to_bytes(transaction_hash)

    statement = _process_columns(columns).where(
        ContractInternalTransactions.transaction_hash == hex_str_to_bytes(transaction_hash)
    )

    raw_transactions = session.exec(statement).all()
    return [_convert_to_response_model(tx) for tx in raw_transactions]


def get_trace_struct_with_contract_by_hash(
    session: Session, transaction_hash: Union[str, bytes], columns: ColumnType = "*"
) -> List[InternalTransactionResponse]:
    """Get internal transactions by transaction hash

    Args:
        session: SQLModel session
        transaction_hash: Transaction hash in hex string format
        columns: Can be "*" for all columns, single column name, or list of column names
                Examples:
                - "*": select all columns
                - "block_number": select only block number
                - "block_number,hash": select block number and hash columns
                - ["block_number", "hash"]: select block number and hash columns

    Returns:
        List[InternalTransactionResponse]: List of standardized transaction responses
    """
    if isinstance(transaction_hash, str):
        transaction_hash = hex_str_to_bytes(transaction_hash)

    statement = _process_columns(columns).where(
        ContractInternalTransactions.transaction_hash == hex_str_to_bytes(transaction_hash)
    )

    raw_transactions = session.exec(statement).all()
    return [_convert_to_response_model(tx) for tx in raw_transactions]
