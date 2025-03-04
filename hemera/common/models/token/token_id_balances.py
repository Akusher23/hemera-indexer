from datetime import datetime
from decimal import Decimal
from typing import Optional

from sqlalchemy import Column, func
from sqlalchemy.dialects.postgresql import BOOLEAN, TIMESTAMP
from sqlalchemy.sql import text
from sqlmodel import Field, Index

from hemera.common.models import HemeraModel, general_converter
from hemera.indexer.domains.current_token_id_balance import CurrentTokenIdBalance
from hemera.indexer.domains.token_id_balance import TokenIdBalance


class AddressTokenIdBalances(HemeraModel, table=True):
    __tablename__ = "address_token_id_balances"

    # Primary keys
    address: bytes = Field(primary_key=True)
    token_id: Decimal = Field(primary_key=True, max_digits=78)
    token_address: bytes = Field(primary_key=True)
    block_number: int = Field(primary_key=True)
    block_timestamp: datetime = Field(primary_key=True)

    # Token info
    balance: Optional[Decimal] = Field(default=None, max_digits=100)

    # Metadata
    create_time: datetime = Field(
        default_factory=datetime.utcnow, sa_column=Column(TIMESTAMP, server_default=func.now())
    )
    update_time: datetime = Field(
        default_factory=datetime.utcnow, sa_column=Column(TIMESTAMP, server_default=func.now())
    )
    reorg: bool = Field(default=False, sa_column=Column(BOOLEAN, server_default=text("false")))

    @staticmethod
    def model_domain_mapping():
        return [
            {
                "domain": TokenIdBalance,
                "conflict_do_update": True,
                "update_strategy": None,
                "converter": general_converter,
            }
        ]

    __table_args__ = (
        Index(
            "token_balance_id_address_id_number_index",
            text("address, token_address, token_id, block_number DESC"),
        ),
    )


class CurrentTokenIdBalances(HemeraModel, table=True):
    __tablename__ = "address_current_token_id_balances"

    # Primary key fields
    address: bytes = Field(primary_key=True)
    token_id: Decimal = Field(primary_key=True, max_digits=78)
    token_address: bytes = Field(primary_key=True)

    # Token fields
    balance: Optional[Decimal] = Field(default=None, max_digits=100)

    # Block related fields
    block_number: Optional[int] = Field(default=None)
    block_timestamp: Optional[datetime] = Field(default=None)

    # Metadata fields
    create_time: Optional[datetime] = Field(default_factory=datetime.utcnow)
    update_time: Optional[datetime] = Field(default_factory=datetime.utcnow)
    reorg: Optional[bool] = Field(default=False)

    @staticmethod
    def model_domain_mapping():
        return [
            {
                "domain": CurrentTokenIdBalance,
                "conflict_do_update": True,
                "update_strategy": "EXCLUDED.block_number >= address_current_token_balances.block_number",
                "converter": general_converter,
            }
        ]

    __table_args__ = (
        Index("current_token_id_balances_token_address_balance_of_index", text("token_address, balance DESC")),
        Index(
            "current_token_balances_token_address_id_balance_of_index",
            text("token_address, token_id, balance DESC"),
        ),
    )
