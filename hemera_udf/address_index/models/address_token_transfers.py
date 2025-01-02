from datetime import datetime
from decimal import Decimal
from typing import Optional

from sqlalchemy import Column
from sqlalchemy.dialects.postgresql import BYTEA, INTEGER, NUMERIC, SMALLINT, TIMESTAMP
from sqlmodel import Field, SQLModel

from hemera.common.models import HemeraModel, general_converter
from hemera_udf.address_index.domains import AddressTokenTransfer


class AddressTokenTransfers(HemeraModel, table=True):
    """Model for indexing token transfers by address"""

    __tablename__ = "address_token_transfers"

    # Primary keys
    address: bytes = Field(sa_column=Column(BYTEA, primary_key=True))
    block_number: int = Field(sa_column=Column(INTEGER, primary_key=True))
    log_index: int = Field(sa_column=Column(INTEGER, primary_key=True))
    transaction_hash: bytes = Field(sa_column=Column(BYTEA, primary_key=True))
    block_timestamp: datetime = Field(sa_column=Column(TIMESTAMP, primary_key=True))
    block_hash: bytes = Field(sa_column=Column(BYTEA, primary_key=True))

    # Transfer data
    token_address: Optional[bytes] = Field(sa_column=Column(BYTEA))
    related_address: Optional[bytes] = Field(sa_column=Column(BYTEA))
    transfer_type: Optional[int] = Field(sa_column=Column(SMALLINT))
    value: Optional[Decimal] = Field(sa_column=Column(NUMERIC(100)))

    # Metadata
    create_time: datetime = Field(default_factory=datetime.utcnow)
    update_time: datetime = Field(default_factory=datetime.utcnow)

    @staticmethod
    def model_domain_mapping():
        return [
            {
                "domain": AddressTokenTransfer,
                "conflict_do_update": True,
                "update_strategy": None,
                "converter": general_converter,
            }
        ]
