#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time  2024/12/22
# @Author  ideal93
# @File  transaction_utils.py
# @Brief

from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple, Union

from pydantic import BaseModel, Field
from sqlalchemy import and_, desc, literal, select, true
from sqlmodel import Session, and_, desc, func, or_, select
from typing_extensions import Literal, Tuple

from hemera.app.api.routes.enricher.address_enricher import Address
from hemera.app.api.routes.helper import ColumnType, process_columns
from hemera.app.api.routes.helper.address import get_txn_cnt_by_address
from hemera.common.enumeration.txn_type import AddressTransactionType
from hemera.common.models.address.address_transactions import AddressTransactions
from hemera.common.models.base.transactions import Transactions
from hemera.common.models.stats.daily_boards_stats import DailyBoardsStats
from hemera.common.models.stats.daily_transactions_stats import DailyTransactionsStats
from hemera.common.models.utils.scheduled_metadata import ScheduledMetadata
from hemera.common.utils.format_utils import bytes_to_hex_str, hex_str_to_bytes

# Constants
MAX_ADDRESS_TXN_COUNT = 100000


class TransactionDetail(BaseModel):
    # Basic Transaction Info
    hash: str
    nonce: int
    transaction_index: int
    transaction_type: int

    # Address Info
    from_address: str
    to_address: str
    from_addr: Address
    to_addr: Address

    # Value and Contract Info
    value: str
    display_value: str

    # Gas Related Fields
    gas: int
    gas_price: str
    max_fee_per_gas: Optional[str]
    max_priority_fee_per_gas: Optional[str]
    gas_price_gwei: str
    gas_fee_token_price: Optional[str]

    # Input and Function Info
    input: str
    input_data: List[dict]
    method: Optional[str]
    method_id: Optional[str]
    function_name: Optional[str]
    function_unsigned: Optional[str]
    full_function_name: Optional[str]

    # Receipt Info
    receipt_cumulative_gas_used: int
    receipt_gas_used: int
    receipt_contract_address: Optional[str]
    receipt_root: Optional[str]
    receipt_status: int
    receipt_effective_gas_price: int

    # L1 Specific Fields
    receipt_l1_fee: Optional[str]
    receipt_l1_gas_used: Optional[str]
    receipt_l1_gas_price: Optional[str]
    receipt_l1_fee_scalar: Optional[str]

    # Block Info
    block_timestamp: datetime
    block_number: int
    block_hash: str

    # Transaction Fee Info
    transaction_fee: str
    transaction_fee_usd: str
    total_transaction_fee: str
    total_transaction_fee_usd: str
    value_usd: str


class TransactionAbbr(BaseModel):
    """Standardized response model for transactions"""

    hash: Optional[str] = None
    transaction_index: Optional[int] = None

    from_address: Optional[str] = None
    to_address: Optional[str] = None
    value: Optional[Decimal] = 0
    transaction_fee: Optional[Decimal] = None
    receipt_status: Optional[int] = None

    block_number: Optional[int] = None
    block_hash: Optional[str] = None
    block_timestamp: Optional[datetime] = None
    method_id: Optional[str] = None

    @staticmethod
    def from_db_model(transaction: Union[Transactions, AddressTransactions]) -> "TransactionAbbr":
        common_fields = {
            "value": transaction.value,
            "receipt_status": transaction.receipt_status,
            "block_number": transaction.block_number,
            "block_timestamp": transaction.block_timestamp,
            "transaction_index": transaction.transaction_index,
            "block_hash": bytes_to_hex_str(transaction.block_hash) if transaction.block_hash else None,
        }

        if isinstance(transaction, Transactions):
            common_fields["hash"] = bytes_to_hex_str(transaction.hash)
            common_fields["from_address"] = bytes_to_hex_str(transaction.from_address)
            common_fields["to_address"] = bytes_to_hex_str(transaction.to_address)
            receipt_gas_used = transaction.receipt_gas_used or Decimal(0)
            gas_price = transaction.gas_price or Decimal(0)
            common_fields["transaction_fee"] = receipt_gas_used * gas_price
            common_fields["method_id"] = transaction.method_id

        else:  # AddressTransactions
            common_fields["hash"] = bytes_to_hex_str(transaction.transaction_hash)
            common_fields["from_address"] = (
                bytes_to_hex_str(transaction.address)
                if transaction.txn_type in [AddressTransactionType.SENDER.value, AddressTransactionType.CREATOR.value]
                else bytes_to_hex_str(transaction.related_address)
            )
            common_fields["to_address"] = (
                bytes_to_hex_str(transaction.related_address)
                if transaction.txn_type in [AddressTransactionType.SENDER.value, AddressTransactionType.CREATOR.value]
                else bytes_to_hex_str(transaction.address)
            )
            common_fields["transaction_fee"] = transaction.transaction_fee
            common_fields["method_id"] = transaction.method

        return TransactionAbbr(**common_fields)


def _process_columns(columns: ColumnType):
    return process_columns(Transactions, columns)


def _process_address_columns(columns: ColumnType):
    return process_columns(AddressTransactions, columns)


class GasStats(BaseModel):
    gas_price_avg: float = Field(None, description="Average gas price")
    gas_price_max: float = Field(None, description="Maximum gas price")
    gas_price_min: float = Field(None, description="Minimum gas price")
    gas_fee_avg: float = Field(None, description="Average gas fee")
    gas_fee_max: float = Field(None, description="Maximum gas fee")
    gas_fee_min: float = Field(None, description="Minimum gas fee")


def get_gas_stats(session: Session, duration: timedelta) -> GasStats:
    """
    Calculate the average, maximum, and minimum values of gas_price and gas_fee (i.e. gas_price * receipt_cumulative_gas_used)
    within the specified duration (up to 1 hour) based on the latest block_timestamp in the database.

    Args:
        session (Session): SQLModel session object.
        duration (timedelta): Time duration for which to calculate the metrics, must not exceed 1 hour.

    Returns:
        GasStats: A Pydantic model instance containing the aggregated values for gas_price and gas_fee.

    Raises:
        ValueError: If the provided duration exceeds 1 hour.
    """
    if duration > timedelta(hours=1):
        raise ValueError("duration must not exceed 1 hour")

    # Retrieve the latest block_timestamp from the Transactions table
    latest_time_stmt = select(func.max(Transactions.block_timestamp))
    latest_time = session.exec(latest_time_stmt).one()
    if latest_time is None:
        # Return a GasStats instance with None for all values if there is no data in the database
        return GasStats()

    start_time = latest_time - duration

    # Construct the SQL statement to calculate aggregated metrics for gas_price and gas_fee
    stmt = select(
        func.avg(Transactions.gas_price),
        func.max(Transactions.gas_price),
        func.min(Transactions.gas_price),
        func.avg(Transactions.gas_price * Transactions.receipt_cumulative_gas_used),
        func.max(Transactions.gas_price * Transactions.receipt_cumulative_gas_used),
        func.min(Transactions.gas_price * Transactions.receipt_cumulative_gas_used),
    ).where(Transactions.block_timestamp >= start_time)

    result = session.exec(stmt).one()

    return GasStats(
        gas_price_avg=result[0],
        gas_price_max=result[1],
        gas_price_min=result[2],
        gas_fee_avg=result[3],
        gas_fee_max=result[4],
        gas_fee_min=result[5],
    )


def get_latest_txn_count(session: Session, duration: timedelta) -> int:
    """
    Calculate the number of transactions within the specified duration (up to 1 hour)
    based on the latest block_timestamp in the database.

    Args:
        session (Session): SQLModel session.
        duration (timedelta): Time duration for which to calculate the transaction count.
                              Must not exceed 1 hour.

    Returns:
        int: Number of transactions within the specified time duration.

    Raises:
        ValueError: If the provided duration exceeds 1 hour.
    """
    if duration > timedelta(hours=1):
        raise ValueError("duration must not exceed 1 hour")

    # Retrieve the latest block_timestamp from the Transactions table
    latest_time_stmt = select(func.max(Transactions.block_timestamp))
    latest_time = session.exec(latest_time_stmt).one()
    if latest_time is None:
        return 0

    start_time = latest_time - duration

    # Count transactions with block_timestamp greater than or equal to start_time
    count_stmt = select(func.count()).where(Transactions.block_timestamp >= start_time)
    count = session.exec(count_stmt).one()
    return count


def get_total_txn_count(session: Session) -> int:
    """
    Get the total transaction count, estimating based on the last known block date and recent transactions.

    Args:
        session: SQLModel session

    Returns:
        Total estimated transaction count
    """
    # Get the latest block date and cumulative count
    latest_record = session.exec(
        select(DailyTransactionsStats.block_date, DailyTransactionsStats.total_cnt).order_by(
            DailyTransactionsStats.block_date.desc()
        )
    ).first()

    # Check if the query returned a result
    if latest_record is None:
        return session.exec(select(func.count()).select_from(Transactions)).first()

    block_date, cumulate_count = latest_record

    current_time = datetime.utcnow()
    ten_minutes_ago = current_time - timedelta(minutes=10)

    latest_10_min_txn_cnt = session.exec(
        select(func.count()).select_from(Transactions).where(Transactions.block_timestamp >= ten_minutes_ago)
    ).first()

    # Convert block_date (datetime.date) to datetime.datetime (assuming midnight as the time)
    block_datetime = datetime.combine(block_date, datetime.min.time())

    avg_txn_per_minute = latest_10_min_txn_cnt / 10
    minutes_since_last_block = int((current_time - block_datetime).total_seconds() / 60)

    estimated_txn = int(avg_txn_per_minute * minutes_since_last_block)

    return estimated_txn + cumulate_count


def get_last_transaction(session: Session, columns: ColumnType = "*") -> Optional[Transactions]:
    """Get the latest transaction

    Args:
        session: SQLModel session
        columns: Can be "*" for all columns, single column name, or list of column names
                Examples:
                - "*": select all columns
                - "block_timestamp": select only timestamp
                - "block_number,hash": select block number and hash columns
                - ["block_number", "hash"]: select block number and hash columns

    Returns:
        Optional[Transactions]: Latest transaction or None
        When specific columns are selected, other attributes will raise AttributeError when accessed
    """
    statement = _process_columns(columns)
    statement = statement.order_by(desc(Transactions.block_number), desc(Transactions.transaction_index))
    return session.exec(statement).first()


def get_tps_latest_10min(session: Session, timestamp: datetime) -> float:
    """Calculate transactions per second for the last 10 minutes

    Args:
        session: SQLModel session
        timestamp: Reference timestamp

    Returns:
        float: Average TPS over the last 10 minutes
    """
    statement = select(func.count()).where(Transactions.block_timestamp >= (timestamp - timedelta(minutes=10)))
    count = session.exec(statement).one()
    return float(count / 600)


def get_address_transaction_count(session: Session, address: str, use_index: bool = True) -> int:
    """Get total transaction count for an address

    Args:
        session: SQLModel session
        address: Address in hex string format
        use_index: Whether to use address index table (default: True)

    Returns:
        int: Total number of transactions for the address

    Raises:
        ValueError: If address format is invalid
    """
    statement = select(func.max(ScheduledMetadata.last_data_timestamp))
    last_timestamp = session.exec(statement).one()
    bytes_address = hex_str_to_bytes(address)

    past_txn_count = get_txn_cnt_by_address(session, address) or 0

    if past_txn_count > MAX_ADDRESS_TXN_COUNT:
        return past_txn_count

    if use_index:
        statement = select(func.count()).where(
            and_(
                AddressTransactions.block_timestamp >= last_timestamp if last_timestamp is not None else True,
                AddressTransactions.address == bytes_address,
            )
        )
    else:
        statement = select(func.count()).where(
            and_(
                Transactions.block_timestamp >= last_timestamp if last_timestamp is not None else True,
                or_(Transactions.from_address == bytes_address, Transactions.to_address == bytes_address),
            )
        )

    recently_txn_count = session.exec(statement).one()
    return past_txn_count + recently_txn_count


def get_total_transaction_count(session: Session) -> int:
    """Get estimated total transaction count

    Args:
        session: SQLModel session

    Returns:
        int: Estimated total number of transactions
    """
    statement = (
        select(DailyTransactionsStats.block_date, DailyTransactionsStats.total_cnt)
        .order_by(desc(DailyTransactionsStats.block_date))
        .limit(1)
    )

    latest_record = session.exec(statement).first()

    if latest_record is None:
        statement = select(func.count()).select_from(Transactions)
        return session.exec(statement).one()

    block_date, cumulate_count = latest_record
    current_time = datetime.utcnow()
    ten_minutes_ago = current_time - timedelta(minutes=10)

    statement = select(func.count()).where(Transactions.block_timestamp >= ten_minutes_ago)
    latest_10_min_txn_cnt = session.exec(statement).one()

    avg_txn_per_minute = latest_10_min_txn_cnt / 10
    block_date_as_datetime = datetime.combine(block_date, datetime.min.time())
    minutes_since_last_block = int((current_time - block_date_as_datetime).total_seconds() / 60)
    estimated_txn = int(avg_txn_per_minute * minutes_since_last_block)

    return estimated_txn + cumulate_count


def get_transactions_by_condition(
    session: Session,
    filter_condition: Optional[Any] = None,
    columns: ColumnType = "*",
    limit: Optional[int] = None,
    offset: Optional[int] = None,
) -> List[Transactions]:
    """Get transactions by condition with pagination support

    Args:
        session: SQLModel session
        filter_condition: SQL filter condition, defaults to None (no filter)
        columns: Can be "*" for all columns, single column name, or list of column names
                Examples:
                - "*": select all columns
                - "block_number": select only block number
                - "block_number,hash": select block number and hash columns
                - ["block_number", "hash"]: select block number and hash columns
        limit: Max number of transactions to return
        offset: Number of transactions to skip

    Returns:
        List[Transactions]: List of matching transactions
        When specific columns are selected, other attributes will raise AttributeError when accessed
    """
    statement = _process_columns(columns)

    if filter_condition is not None:
        statement = statement.where(filter_condition)

    statement = statement.order_by(desc(Transactions.block_number), desc(Transactions.transaction_index))

    if limit is not None:
        statement = statement.limit(limit)
    if offset is not None:
        statement = statement.offset(offset)

    return session.exec(statement).all()


def get_transactions_count_by_condition(session: Session, filter_condition: Optional[Any] = None) -> int:
    """Get count of transactions matching the given condition

    Args:
        session: SQLModel session
        filter_condition: SQL filter condition, defaults to None (no filter)

    Returns:
        int: Number of matching transactions
    """
    statement = select(func.count()).select_from(Transactions)

    if filter_condition is not None:
        statement = statement.where(filter_condition)

    return session.exec(statement).one()


def get_transactions_and_total_count_by_condition(
    session: Session,
    filter_condition: Optional[Any] = None,
    columns: ColumnType = "*",
    limit: Optional[int] = None,
    offset: Optional[int] = None,
) -> (List[TransactionAbbr], int):
    """Get transactions and total count by condition

    Args:
        session: SQLModel session
        filter_condition: SQL filter condition, defaults to None (no filter)
        columns: Columns to select
        limit: Max number of transactions to return
        offset: Number of transactions to skip

    Returns:
        (List[TransactionAbbr], int): List of standardized transaction responses and total count
    """

    transactions = get_transactions_by_condition(
        session=session,
        filter_condition=filter_condition,
        columns=columns,
        limit=limit,
        offset=offset,
    )
    total_count = get_transactions_count_by_condition(session=session, filter_condition=filter_condition)

    return [TransactionAbbr.from_db_model(tx) for tx in transactions], total_count


def get_transactions_by_address(
    session: Session,
    address: Union[str, bytes],
    direction: Optional[Literal["from", "to", "both"]] = "both",
    limit: Optional[int] = None,
    offset: Optional[int] = None,
    use_address_index: bool = False,
) -> List[TransactionAbbr]:
    """Get transactions by address with option to use address index

    Args:
        session: SQLModel session
        address: Address in hex string format
        direction: Filter direction - "from", "to", or "both" (default)
        columns: Columns to select
        limit: Max number of transactions to return
        offset: Number of transactions to skip
        use_address_index: Whether to use address index table (default: False)

    Returns:
        List[TransactionAbbr]: List of standardized transaction responses
    """
    if use_address_index:
        raw_transactions = _get_transactions_by_address_using_address_index(
            session=session,
            address=address,
            direction=direction,
            columns="*",
            limit=limit,
            offset=offset,
        )
    else:
        raw_transactions = _get_transactions_by_address_native(
            session=session,
            address=address,
            direction=direction,
            columns="*",
            limit=limit,
            offset=offset,
        )

    return [TransactionAbbr.from_db_model(tx) for tx in raw_transactions]


def _get_transactions_by_address_using_address_index(
    session: Session,
    address: Union[str, bytes],
    direction: Optional[Literal["from", "to", "both"]] = "both",
    columns: ColumnType = "*",
    limit: Optional[int] = None,
    offset: Optional[int] = None,
) -> List[AddressTransactions]:
    """Get transactions by address using address index table

    Args:
        session: SQLModel session
        address: Address in hex string format
        direction: Filter direction - "from", "to", or "both" (default)
        columns: Columns to select
        limit: Max number of transactions to return
        offset: Number of transactions to skip

    Returns:
        List[AddressTransactions]: List of matching transactions
    """
    if isinstance(address, str):
        address = hex_str_to_bytes(address)

    statement = _process_address_columns(columns).where(AddressTransactions.address == address)

    if direction == "from":
        statement = statement.where(AddressTransactions.is_sender == True)
    elif direction == "to":
        statement = statement.where(AddressTransactions.is_sender == False)
    # For "both", no additional filter needed

    statement = statement.order_by(desc(AddressTransactions.block_number), desc(AddressTransactions.transaction_index))

    if limit is not None:
        statement = statement.limit(limit)
    if offset is not None:
        statement = statement.offset(offset)

    return session.exec(statement).all()


def _get_transactions_by_address_native(
    session: Session,
    address: Union[str, bytes],
    direction: Optional[Literal["from", "to", "both"]] = "both",
    columns: ColumnType = "*",
    limit: Optional[int] = None,
    offset: Optional[int] = None,
) -> List[Transactions]:
    """Get transactions by address using native transactions table

    Args:
        session: SQLModel session
        address: Address in hex string format
        direction: Filter direction - "from", "to", or "both" (default)
        columns: Columns to select
        limit: Max number of transactions to return
        offset: Number of transactions to skip

    Returns:
        List[Transactions]: List of matching transactions
    """
    if isinstance(address, str):
        address = hex_str_to_bytes(address)

    statement = _process_columns(columns)

    if direction == "from":
        statement = statement.where(Transactions.from_address == address)
    elif direction == "to":
        statement = statement.where(Transactions.to_address == address)
    else:  # both
        statement = statement.where(
            or_(
                Transactions.from_address == address,
                Transactions.to_address == address,
            )
        )

    statement = statement.order_by(desc(Transactions.block_number), desc(Transactions.transaction_index))

    if limit is not None:
        statement = statement.limit(limit)
    if offset is not None:
        statement = statement.offset(offset)

    return session.exec(statement).all()


def get_transactions_count_by_address(
    session: Session,
    address: Union[str, bytes],
    direction: Optional[Literal["from", "to", "both"]] = "both",
    use_address_index: bool = False,
) -> int:
    """Get count of transactions by address

    Args:
        session: SQLModel session
        address: Address in hex string format
        direction: Filter direction - "from", "to", or "both" (default)
        use_address_index: Whether to use address index table (default: False)

    Returns:
        int: Count of transactions
    """
    if use_address_index:
        return _get_transactions_count_by_address_using_address_index(
            session=session,
            address=address,
            direction=direction,
        )
    else:
        return _get_transactions_count_by_address_native(
            session=session,
            address=address,
            direction=direction,
        )


def _get_transactions_count_by_address_using_address_index(
    session: Session,
    address: Union[str, bytes],
    direction: Optional[Literal["from", "to", "both"]] = "both",
) -> int:
    """Get count of transactions by address using address index

    Args:
        session: SQLModel session
        address: Address in hex string format
        direction: Filter direction - "from", "to", or "both" (default)

    Returns:
        int: Count of transactions
    """
    if isinstance(address, str):
        address = hex_str_to_bytes(address)

    statement = select(func.count()).select_from(AddressTransactions).where(AddressTransactions.address == address)

    if direction == "from":
        statement = statement.where(AddressTransactions.is_sender == True)
    elif direction == "to":
        statement = statement.where(AddressTransactions.is_sender == False)

    return session.exec(statement).first()


def _get_transactions_count_by_address_native(
    session: Session,
    address: Union[str, bytes],
    direction: Optional[Literal["from", "to", "both"]] = "both",
) -> int:
    """Get count of transactions by address using native table

    Args:
        session: SQLModel session
        address: Address in hex string format
        direction: Filter direction - "from", "to", or "both" (default)

    Returns:
        int: Count of transactions
    """
    if isinstance(address, str):
        address = hex_str_to_bytes(address)

    statement = select(func.count()).select_from(Transactions)

    if direction == "from":
        statement = statement.where(Transactions.from_address == address)
    elif direction == "to":
        statement = statement.where(Transactions.to_address == address)
    else:  # both
        statement = statement.where(
            or_(
                Transactions.from_address == address,
                Transactions.to_address == address,
            )
        )

    return session.exec(statement).first()


def get_transaction_by_hash(session: Session, hash: str) -> Optional[TransactionDetail]:
    """Get transaction by transaction hash

    Args:
        session: SQLModel session
        hash: Transaction hash in hex string format

    Returns:
        Optional[TransactionDetail]: Matching transaction or None
    """
    transaction = _get_transaction_by_hash(session, hash)
    if transaction is None:
        return None


def _get_transaction_by_hash(
    session: Session, hash: Union[str, bytes], columns: ColumnType = "*"
) -> Optional[Transactions]:
    """Get transaction by transaction hash

    Args:
        session: SQLModel session
        hash: Transaction hash (hex string) or bytes
        columns: Can be "*" for all columns, single column name, or list of column names
                Examples:
                - "*": select all columns
                - "block_number": select only block number
                - "block_number,hash": select block number and hash columns
                - ["block_number", "hash"]: select block number and hash columns

    Returns:
        Optional[Transactions]: Matching transaction or None
        When specific columns are selected, other attributes will raise AttributeError when accessed

    Raises:
        ValueError: If hash format is invalid
    """
    if isinstance(hash, str):
        hash = hex_str_to_bytes(hash)
    statement = _process_columns(columns)
    statement = statement.where(Transactions.hash == hash)
    return session.exec(statement).first()


def get_gas_stats_list(
    session: Session, duration: timedelta, interval: timedelta, latest_timestamp: Optional[datetime] = None
) -> List[Tuple[datetime, GasStats]]:
    """
    Returns a list of GasStats, each corresponding to a time bucket of aggregated statistics,
    for the given duration and interval (e.g., 12:01, 12:02, 12:03).
    The duration must not exceed 1 hour.
    Note: This example is for PostgreSQL; adjustments may be needed for other databases.
    """
    if duration > timedelta(hours=1):
        raise ValueError("duration must not exceed 1 hour")

    if latest_timestamp is None:
        # Retrieve the latest block_timestamp from the Transactions table
        latest_time_stmt = select(func.max(Transactions.block_timestamp))
        latest_time = session.exec(latest_time_stmt).one()
        if latest_time is None:
            # Return a GasStats instance with None for all values if there is no data in the database
            return List[Tuple[datetime, GasStats]]()
    else:
        latest_time = latest_timestamp
    latest_time = latest_time.replace(microsecond=0).replace(second=0)
    start_time = latest_time - duration
    interval_seconds = interval.total_seconds()
    start_epoch = start_time.timestamp()

    bucket_expr = func.to_timestamp(
        func.floor((func.extract("epoch", Transactions.block_timestamp) - start_epoch) / interval_seconds)
        * interval_seconds
        + start_epoch
    ).label("bucket")

    stmt = (
        select(
            bucket_expr,
            func.avg(Transactions.gas_price).label("gas_price_avg"),
            func.max(Transactions.gas_price).label("gas_price_max"),
            func.min(Transactions.gas_price).label("gas_price_min"),
            func.avg(Transactions.gas_price * Transactions.receipt_cumulative_gas_used).label("gas_fee_avg"),
            func.max(Transactions.gas_price * Transactions.receipt_cumulative_gas_used).label("gas_fee_max"),
            func.min(Transactions.gas_price * Transactions.receipt_cumulative_gas_used).label("gas_fee_min"),
        )
        .where(Transactions.block_timestamp >= start_time, Transactions.block_timestamp < latest_time)
        .group_by(bucket_expr)
        .order_by(bucket_expr)
    )

    results = session.exec(stmt).all()

    stats_list = [
        (
            row.bucket,
            GasStats(
                gas_price_avg=row.gas_price_avg,
                gas_price_max=row.gas_price_max,
                gas_price_min=row.gas_price_min,
                gas_fee_avg=row.gas_fee_avg,
                gas_fee_max=row.gas_fee_max,
                gas_fee_min=row.gas_fee_min,
            ),
        )
        for row in results
    ]
    return stats_list


def get_transaction_count_stats_list(
    session: Session, duration: timedelta, interval: timedelta, latest_timestamp: Optional[datetime] = None
) -> List[Tuple[datetime, int]]:
    """
    Returns a list of tuples, each containing a bucket start datetime and the average transaction
    count per second for that bucket. The buckets are defined over the given duration and interval.
    The duration must not exceed 1 hour.
    Note: This example is for PostgreSQL; adjustments may be needed for other databases.
    """
    if duration > timedelta(hours=1):
        raise ValueError("duration must not exceed 1 hour")

    if latest_timestamp is None:
        # Retrieve the latest block_timestamp from the Transactions table
        latest_time_stmt = select(func.max(Transactions.block_timestamp))
        latest_time = session.exec(latest_time_stmt).one()
        if latest_time is None:
            # Return an empty list if there is no data in the database
            return []
    else:
        latest_time = latest_timestamp
    latest_time = latest_time.replace(microsecond=0).replace(second=0)
    start_time = latest_time - duration
    interval_seconds = interval.total_seconds()
    start_epoch = start_time.timestamp()

    # Build a bucket expression:
    # Convert block_timestamp to epoch seconds, subtract start_epoch, divide by interval_seconds,
    # floor the result, multiply back by interval_seconds, add start_epoch, and convert back to timestamp.
    bucket_expr = func.to_timestamp(
        func.floor((func.extract("epoch", Transactions.block_timestamp) - start_epoch) / interval_seconds)
        * interval_seconds
        + start_epoch
    ).label("bucket")

    # Construct the SQL statement:
    # For each bucket, count the transactions and calculate the average per second (count / interval_seconds)
    stmt = (
        select(bucket_expr, (func.count(Transactions.hash)).label("tx_count"))
        .where(Transactions.block_timestamp >= start_time, Transactions.block_timestamp < latest_time)
        .group_by(bucket_expr)
        .order_by(bucket_expr)
    )

    results = session.exec(stmt).all()

    # Build a list of tuples (bucket timestamp, average transaction count per second)
    stats_list: List[Tuple[datetime, int]] = [(row.bucket, row.tx_count) for row in results]
    return stats_list


class SmartContractMetric(BaseModel):
    contract_address: str = Field(..., description="Smart contract address")
    minute_transaction_count: int = Field(..., description="Transaction count")


def get_top_contracts_transaction_count_list(
    session: Session, duration: timedelta, interval: timedelta, latest_timestamp: Optional[datetime] = None
) -> List[Tuple[datetime, List[SmartContractMetric]]]:
    if duration > timedelta(hours=1):
        raise ValueError("duration must not exceed 1 hour")

    # Determine the latest time.
    if latest_timestamp is None:
        latest_time_stmt = select(func.max(AddressTransactions.block_timestamp))
        latest_time = session.exec(latest_time_stmt).one()
        if latest_time is None:
            return []
    else:
        latest_time = latest_timestamp

    # Truncate latest_time to minute precision.
    latest_time = latest_time.replace(microsecond=0).replace(second=0)
    start_time = latest_time - duration
    interval_seconds = interval.total_seconds()

    top_20_addresses_stmt = (
        select(DailyBoardsStats.key)
        .where(
            DailyBoardsStats.board_id == "top_contract_transactions",
            DailyBoardsStats.block_date
            == (
                select(func.max(DailyBoardsStats.block_date))
                .where(DailyBoardsStats.board_id == "top_contract_transactions")
                .scalar_subquery()
            ),
        )
        .order_by(desc(DailyBoardsStats.count))
        .limit(20)
    )
    top_20_addresses = session.exec(top_20_addresses_stmt).all()
    if not top_20_addresses:
        return []

    stmt = (
        select(
            func.date_trunc("minute", AddressTransactions.block_timestamp).label("bucket"),
            AddressTransactions.address,
            (func.count(AddressTransactions.transaction_hash)).label("tx_count_per_minute"),
        )
        .where(
            AddressTransactions.block_timestamp >= start_time,
            AddressTransactions.block_timestamp < latest_time,
            AddressTransactions.address.in_([hex_str_to_bytes(addr) for addr in top_20_addresses]),
        )
        .group_by("bucket", AddressTransactions.address)
        .order_by("bucket")
    )

    results = session.exec(stmt).all()

    all_buckets = [
        start_time + timedelta(seconds=interval_seconds * i)
        for i in range(int(duration.total_seconds() / interval_seconds))
    ]

    buckets: Dict[datetime, Dict[str, float]] = {
        bucket: {addr: 0.0 for addr in top_20_addresses} for bucket in all_buckets
    }

    for row in results:
        bucket_minute = row.bucket
        address = bytes_to_hex_str(row.address)
        tx_count_per_minute = row.tx_count_per_minute

        for bucket in all_buckets:
            if bucket <= bucket_minute < bucket + interval:
                if address in buckets[bucket]:
                    buckets[bucket][address] += tx_count_per_minute
                break

    stats_list: List[Tuple[datetime, List[SmartContractMetric]]] = []
    for bucket, address_counts in sorted(buckets.items(), key=lambda x: x[0]):
        metrics = [
            SmartContractMetric(contract_address=address, minute_transaction_count=tx_count)
            for address, tx_count in sorted(address_counts.items())
        ]
        stats_list.append((bucket, metrics))

    return stats_list


def get_recent_1_minutes_average_transactions(
    session: Session, latest_timestamp: Optional[datetime] = None
) -> List[SmartContractMetric]:
    top_20_addresses_subquery = (
        select(DailyBoardsStats.key)
        .where(
            DailyBoardsStats.board_id == "top_contract_transactions",
            DailyBoardsStats.block_date
            == (
                select(func.max(DailyBoardsStats.block_date))
                .where(DailyBoardsStats.board_id == "top_contract_transactions")
                .scalar_subquery()
            ),
        )
        .order_by(desc(DailyBoardsStats.count))
        .limit(20)
        .subquery()
    )

    top_20_addresses = session.exec(select(top_20_addresses_subquery.c.key)).all()

    if not top_20_addresses:
        return []

    if latest_timestamp is None:
        latest_time_stmt = select(func.max(AddressTransactions.block_timestamp))
        latest_timestamp = session.exec(latest_time_stmt).one()
        if latest_timestamp is None:
            return []

    latest_timestamp = latest_timestamp.replace(microsecond=0, second=0)
    start_time = latest_timestamp - timedelta(minutes=1)

    stmt = (
        select(
            func.concat("0x", func.encode(AddressTransactions.address, "hex")).label("address"),
            (func.count(AddressTransactions.transaction_hash)).label("avg_tx_per_minute"),
        )
        .where(
            AddressTransactions.block_timestamp >= start_time,
            AddressTransactions.block_timestamp < latest_timestamp,
            func.concat("0x", func.encode(AddressTransactions.address, "hex")).in_(top_20_addresses),
        )
        .group_by(AddressTransactions.address)
    )

    results = session.exec(stmt).all()

    address_to_avg_tx = {row.address: row.avg_tx_per_minute for row in results}

    top_active_contracts = []
    for address in top_20_addresses:
        avg_tx_per_minute = address_to_avg_tx.get(address, 0.0)
        top_active_contracts.append(
            SmartContractMetric(contract_address=address, minute_transaction_count=avg_tx_per_minute)
        )

    return top_active_contracts
