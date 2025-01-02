#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time  2024/12/20 15:01
# @Author  ideal93
# @File  transaction.py
# @Brief
from datetime import datetime, timedelta
from pathlib import Path
from typing import Annotated, List, Optional, Tuple

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlmodel import Session, and_, desc, func, or_, select

from hemera.app.api.deps import ReadSessionDep, get_read_db
from hemera.app.api.routes.enrichers.block_chain_info_service import BlockchainEnricher
from hemera.app.api.routes.helper.address import get_address_display_mapping
from hemera.app.api.routes.helper.decorator import AddressExtraInfo
from hemera.app.api.routes.helper.format import format_coin_value
from hemera.app.api.routes.helper.internal_transaction import (
    InternalTransactionAbbr,
    get_internal_transactions,
    get_internal_transactions_by_address,
    get_internal_transactions_by_block_number,
    get_internal_transactions_by_hash,
    get_internal_transactions_count,
    get_internal_transactions_count_by_address,
    get_internal_transactions_count_by_block_number,
)
from hemera.app.api.routes.helper.log import LogItem, fill_extra_contract_info_to_logs, get_logs_by_hash
from hemera.app.api.routes.helper.price import fill_price_to_transactions
from hemera.app.api.routes.helper.token import TokenInfo, get_tokens_by_token_address
from hemera.app.api.routes.helper.token_transfers import get_token_transfers_by_hash
from hemera.app.api.routes.helper.transaction import TransactionAbbr, TransactionDetail
from hemera.app.core.config import settings
from hemera.common.models.blocks import Blocks
from hemera.common.models.transactions import Transactions
from hemera.common.utils.format_utils import bytes_to_hex_str, hex_str_to_bytes
from hemera.common.utils.web3_utils import valid_hash

router = APIRouter(tags=["transactions"])

from pydantic import BaseModel


class InternalTransactionItem(InternalTransactionAbbr):
    from_address_extra_info: AddressExtraInfo
    to_address_extra_info: AddressExtraInfo


class InternalTransactionResponse(BaseModel):
    data: List[InternalTransactionItem]
    total: int
    max_display: Optional[int] = None
    page: Optional[int] = None
    size: Optional[int] = None


class TransactionItem(TransactionAbbr):
    from_address_extra_info: AddressExtraInfo
    to_address_extra_info: AddressExtraInfo


class TransactionResponse(BaseModel):
    data: List[TransactionItem]
    total: int
    max_display: int
    page: int
    size: int


class TransactionLogsResponse(BaseModel):
    total: int
    data: List[LogItem]


class TokenTransferItem(BaseModel):
    transaction_hash: Optional[str] = None
    log_index: Optional[int] = None
    from_address: Optional[str] = None
    to_address: Optional[str] = None
    token_id: Optional[str] = None
    value: Optional[str] = None
    token_type: Optional[str] = None
    token_address: Optional[str] = None
    from_address_display: Optional[str] = None
    to_address_display: Optional[str] = None
    token_info: Optional[TokenInfo] = None


class TokenTransfersResponse(BaseModel):
    total: int
    data: List[TokenTransferItem]


class TraceItem(BaseModel):
    from_address: str
    to_address: str
    value: Optional[str]
    input: Optional[str]
    output: Optional[str]
    trace_type: str
    call_type: Optional[str]
    gas: Optional[int]
    gas_used: Optional[int]
    error: Optional[str]
    status: Optional[int]
    function_name: Optional[str]
    function_input: Optional[List[dict]]
    function_output: Optional[List[dict]]
    calls: Optional[List["TraceItem"]]


class TransactionTracesResponse(BaseModel):
    data: TraceItem


class EnricherManager:
    _instance = None
    _enricher = None

    @classmethod
    def get_instance(cls) -> "EnricherManager":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def get_enricher(self, session: Session) -> BlockchainEnricher:
        if self._enricher is None:
            self._enricher = BlockchainEnricher(session)
        else:
            self._enricher.session = session
            self._enricher.token_mapper.session = session
            self._enricher.contract_mapper.session = session
        return self._enricher


def get_blockchain_enricher(session: Session = Depends(get_read_db)) -> BlockchainEnricher:
    return EnricherManager.get_instance().get_enricher(session)


BlockchainEnricherDep = Annotated[BlockchainEnricher, Depends(get_blockchain_enricher)]


@router.get("/v1/explorer/internal_transactions", response_model=InternalTransactionResponse)
async def api_get_internal_transactions(
    session: ReadSessionDep,
    enricher: BlockchainEnricherDep,
    page: int = Query(1, gt=0),
    size: int = Query(25, gt=0),
    address: Optional[str] = None,
    block: Optional[int] = None,
):
    if page * size > settings.MAX_INTERNAL_TRANSACTION:
        raise HTTPException(
            status_code=400, detail=f"Showing the last {settings.MAX_INTERNAL_TRANSACTION} records only"
        )

    if address:
        total_count = get_internal_transactions_count_by_address(session, address, block)
        transactions = get_internal_transactions_by_address(
            session, address, block, limit=size, offset=(page - 1) * size
        )
    elif block:
        total_count = get_internal_transactions_count_by_block_number(session, block)
        transactions = get_internal_transactions_by_block_number(session, block, limit=size, offset=(page - 1) * size)
    else:
        total_count = get_internal_transactions_count(session)
        transactions = get_internal_transactions(session, limit=size, offset=(page - 1) * size)

    return InternalTransactionResponse(
        data=enricher.enrich(
            transactions, InternalTransactionItem, {"contract": lambda x: [x.from_address, x.to_address]}
        ),
        total=total_count,
        max_display=min(total_count, settings.MAX_INTERNAL_TRANSACTION),
        page=page,
        size=size,
    )


class TransactionFilterParams:
    def __init__(
        self,
        block: Optional[str] = Query(None, description="Block number or hash"),
        address: Optional[str] = Query(None, description="Filter by address"),
        date: Optional[str] = Query(None, description="Filter by date (format: YYYYMMDD)"),
    ):
        self.block = block
        self.address = address
        self.date = date
        self._validate_filters()

    def _validate_filters(self):
        filter_count = sum(x is not None for x in [self.block, self.address, self.date])

        if filter_count > 1:
            raise HTTPException(status_code=400, detail="Only one filter can be applied: block, address, or date")

        if self.date:
            try:
                datetime.strptime(self.date, "%Y%m%d")
            except ValueError:
                raise HTTPException(status_code=400, detail="Invalid date format")

        if self.block:
            if not self.block.isnumeric() and not valid_hash(self.block):
                raise HTTPException(status_code=400, detail="Invalid block identifier")


@router.get("/v1/explorer/transactions", response_model=TransactionResponse)
async def get_transactions(
    session: ReadSessionDep,
    page: int = Query(1, gt=0),
    size: int = Query(25, gt=0),
    filters: TransactionFilterParams = Depends(),
):
    """Get transactions list with various filters and pagination.

    Args:
        session: Database session
        page: Page number, starting from 1
        size: Items per page
        filters: Transaction filters (block, address, or date)

    Returns:
        TransactionResponse: Paginated transaction list with metadata

    Raises:
        HTTPException: If page*size exceeds limits or invalid parameters
    """
    # Check pagination limits
    max_limit = (
        settings.MAX_TRANSACTION_WITH_CONDITION
        if any([filters.block, filters.address, filters.date])
        else settings.MAX_TRANSACTION
    )
    if page * size > max_limit:
        raise HTTPException(status_code=400, detail=f"Showing the last {max_limit} records only")

    # Get transactions based on filters
    total_records = 0
    transactions = []

    if filters.block:
        total_records, transactions = _get_transactions_by_block(session, filters.block, page, size)
    elif filters.address:
        total_records, transactions = _get_transactions_by_address(session, filters.address, page, size)
    elif filters.date:
        total_records, transactions = _get_transactions_by_date(session, filters.date, page, size)
    else:
        # Get all transactions with pagination
        transactions = session.exec(
            select(Transactions)
            .order_by(desc(Transactions.block_number), desc(Transactions.transaction_index))
            .offset((page - 1) * size)
            .limit(size)
        ).all()
        total_records = _calculate_total_records(session, transactions, page, size)

    fill_price_to_transactions(session, transactions)

    return TransactionResponse(
        data=[TransactionItem(**tx) for tx in transactions],
        total=total_records,
        max_display=min(max_limit, total_records),
        page=page,
        size=size,
    )


def _get_transactions_by_block(
    session: ReadSessionDep, block: str, page: int, size: int
) -> Tuple[int, List[Transactions]]:
    """Get transactions by block number or hash

    Args:
        session: Database session
        block: Block number or hash
        page: Page number
        size: Items per page

    Returns:
        Tuple[int, List[Transactions]]: Total count and list of transactions
    """
    if block.isnumeric():
        # Query by block number
        block_number = int(block)
        chain_block = session.exec(select(Blocks).where(Blocks.number == block_number)).first()
        if not chain_block:
            raise HTTPException(status_code=400, detail="Block not exist")

        transactions = session.exec(
            select(Transactions)
            .where(Transactions.block_number == block_number)
            .order_by(Transactions.block_number.desc())
            .offset((page - 1) * size)
            .limit(size)
        ).all()

        return chain_block.transactions_count, transactions
    else:
        # Query by block hash
        block_hash = hex_str_to_bytes(block)
        chain_block = session.exec(select(Blocks).where(Blocks.hash == block_hash)).first()
        if not chain_block:
            raise HTTPException(status_code=400, detail="Block not exist")

        transactions = session.exec(
            select(Transactions)
            .where(Transactions.block_hash == block_hash)
            .order_by(Transactions.block_number.desc())
            .offset((page - 1) * size)
            .limit(size)
        ).all()

        return chain_block.transactions_count, transactions


def _get_transactions_by_address(
    session: ReadSessionDep, address: str, page: int, size: int
) -> Tuple[int, List[Transactions]]:
    """Get transactions by address

    Args:
        session: Database session
        address: Address to filter by
        page: Page number
        size: Items per page

    Returns:
        Tuple[int, List[Transactions]]: Total count and list of transactions
    """
    address_bytes = hex_str_to_bytes(address.lower())
    address_condition = or_(Transactions.from_address == address_bytes, Transactions.to_address == address_bytes)

    total_records = session.exec(select(func.count()).select_from(Transactions).where(address_condition)).first()

    transactions = session.exec(
        select(Transactions)
        .where(address_condition)
        .order_by(desc(Transactions.block_number), desc(Transactions.block_timestamp))
        .offset((page - 1) * size)
        .limit(size)
    ).all()

    return total_records, transactions


def _get_transactions_by_date(
    session: ReadSessionDep, date: str, page: int, size: int
) -> Tuple[int, List[Transactions]]:
    """Get transactions by date

    Args:
        session: Database session
        date: Date in YYYYMMDD format
        page: Page number
        size: Items per page

    Returns:
        Tuple[int, List[Transactions]]: Total count and list of transactions
    """
    date_obj = datetime.strptime(date, "%Y%m%d")
    start_time = date_obj
    end_time = start_time + timedelta(days=1)

    date_condition = and_(Transactions.block_timestamp >= start_time, Transactions.block_timestamp < end_time)

    total_records = session.exec(select(func.count()).select_from(Transactions).where(date_condition)).first()

    transactions = session.exec(
        select(Transactions)
        .where(date_condition)
        .order_by(desc(Transactions.block_number), desc(Transactions.block_timestamp))
        .offset((page - 1) * size)
        .limit(size)
    ).all()

    return total_records, transactions


def _calculate_total_records(session: ReadSessionDep, transactions: List[Transactions], page: int, size: int) -> int:
    """Calculate total number of records

    Args:
        session: Database session
        transactions: List of transactions from current page
        page: Current page number
        size: Page size

    Returns:
        int: Total number of records
    """
    if len(transactions) > 0 and len(transactions) < size:
        return (page - 1) * size + len(transactions)
    return session.exec(select(func.count()).select_from(Transactions)).first()


async def verify_transaction_hash(
    tx_hash: Annotated[
        str,
        Path(
            title="Transaction Hash",
            description="Ethereum transaction hash (hex format)",
            min_length=66,
            max_length=66,
            pattern="^0x[0-9a-fA-F]{64}$",
        ),
    ]
) -> str:
    """
    Dependency for validating transaction hashes.

    Args:
        tx_hash: Transaction hash to validate

    Returns:
        str: Validated and formatted transaction hash

    Raises:
        HTTPException: If hash format is invalid
    """
    tx_hash = valid_hash(tx_hash)
    if not tx_hash:
        raise HTTPException(status_code=400, detail="Invalid transaction hash format")
    return tx_hash


TransactionHashDep = Annotated[str, Depends(verify_transaction_hash)]


@router.get("/v1/explorer/transaction/{tx_hash}", response_model=TransactionDetail)
async def get_transaction_detail(session: ReadSessionDep, tx_hash: TransactionHashDep):
    """Get detailed information about a specific transaction.

    Args:
        session: Database session
        tx_hash: Transaction hash in hex format

    Raises:
        HTTPException: If transaction not found or invalid hash
    """
    # Validate and format transaction hash
    # Get transaction with basic info
    pass


@router.get("/v1/explorer/transaction/{tx_hash}/logs", response_model=TransactionLogsResponse)
async def get_transaction_logs(session: ReadSessionDep, tx_hash: TransactionHashDep):
    """Get all logs for a specific transaction.

    Args:
        session: Database session
        tx_hash: Transaction hash in hex format

    Raises:
        HTTPException: If invalid hash format or transaction not found
    """

    logs = get_logs_by_hash(session, tx_hash)
    logs_with_contract_info = fill_extra_contract_info_to_logs(session, log_json)
    return TransactionLogsResponse(
        logs_with_contract_info,
        total=len(logs_with_contract_info),
    )


@router.get("/v1/explorer/transaction/{tx_hash}/token_transfers", response_model=TokenTransfersResponse)
async def get_transaction_token_transfers(session: ReadSessionDep, tx_hash: TransactionHashDep):
    """Get all token transfers (ERC20, ERC721, ERC1155) for a specific transaction.

    Args:
        session: Database session
        tx_hash: Transaction hash in hex format

    Raises:
        HTTPException: If invalid hash format
    """
    # Validate and format hash
    token_transfers = get_token_transfers_by_hash(session, tx_hash)
    address_set = set()
    for transfer in token_transfers:
        address_set.add(transfer.from_address)
        address_set.add(transfer.to_address)
    address_display_map = get_address_display_mapping(session, list(address_set))

    tokens = get_tokens_by_token_address(session, [t.token_address for t in token_transfers])
    token_map = {t.address: t for t in tokens}

    def convert_transfer(transfer_obj, token_map):
        item = transfer_obj.model_dump()
        token_address = transfer_obj.token_address
        token = token_map.get(token_address)
        if token:
            token_info = {
                "token_name": token_map.get(token_address).name,
                "token_address": token_address,
                "token_symbol": token_map.get(token_address).symbol,
                "token_logo_url": token_map.get(token_address).icon_url,
            }
        else:
            token_info = {
                "token_name": None,
                "token_address": token_address,
                "token_symbol": None,
                "token_logo_url": None,
            }
        for key, value in item.items():
            if isinstance(value, bytes):
                item[key] = bytes_to_hex_str(value)
        item.update(
            {
                "token": token_info,
                "from_address_display": address_display_map.get(transfer_obj.from_address),
                "to_address_display": address_display_map.get(transfer_obj.to_address),
                "value": format_coin_value(
                    transfer_obj.value,
                    (
                        token_map.get(transfer_obj.token_address).decimals
                        if token_map.get(transfer_obj.token_address)
                        else 18
                    ),
                ),
                "token_id": str(transfer_obj.token_id) if transfer_obj.token_id else None,
            }
        )
        return item

    transfer_items = [convert_transfer(t, token_map) for t in token_transfers]

    return TokenTransfersResponse(
        total=len(transfer_items), data=[TokenTransferItem(**item) for item in transfer_items]
    )


@router.get("/v1/explorer/transaction/{tx_hash}/internal_transactions", response_model=InternalTransactionResponse)
async def get_transaction_internal_transactions(session: ReadSessionDep, tx_hash: str):
    """Get internal transactions for a specific transaction.

    Args:
        session: Database session
        tx_hash: Transaction hash in hex format

    Raises:
        HTTPException: If invalid hash format
    """
    internal_transactions = get_internal_transactions_by_hash(session, tx_hash)

    fill_is_contract_to_transactions(session, internal_transactions)
    fill_address_display_to_transactions(session, internal_transactions)

    return InternalTransactionResponse(
        data=[InternalTransactionItem(**tx) for tx in internal_transactions],
        total=len(internal_transactions),
    )


@router.get("/v1/explorer/transaction/{tx_hash}/traces", response_model=TransactionTracesResponse)
async def get_transaction_traces(session: ReadSessionDep, tx_hash: str):
    """Get detailed trace information for a transaction.

    Args:
        session: Database session
        tx_hash: Transaction hash in hex format

    Raises:
        HTTPException: If invalid hash format or trace not found
    """
    # Validate and format hash
    pass
