#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from sqlalchemy import Column, Index, PrimaryKeyConstraint, func, text
from sqlalchemy.dialects.postgresql import BIGINT, BOOLEAN, BYTEA, NUMERIC, TIMESTAMP

from common.models import HemeraModel, general_converter


class ERC20TokenTransfer(HemeraModel):
    __tablename__ = "erc20_token_transfer"

    address = Column(BYTEA)
    token_address = Column(BYTEA, primary_key=True)
    value = Column(NUMERIC(100), nullable=False)

    block_number = Column(BIGINT)
    block_timestamp = Column(TIMESTAMP)
    transaction_hash = Column(BYTEA, primary_key=True)
    log_index = Column(BIGINT, primary_key=True)

    create_time = Column(TIMESTAMP, server_default=func.now())
    update_time = Column(TIMESTAMP, server_default=func.now())
    reorg = Column(BOOLEAN, server_default=text("false"))

    __table_args__ = (PrimaryKeyConstraint("token_address", "transaction_hash", "log_index"),)

    @staticmethod
    def model_domain_mapping():
        return [
            {
                "domain": "ERC20TokenTransfer",
                "conflict_do_update": True,
                "update_strategy": None,
                "converter": general_converter,
            },
        ]

Index(
    "erc20_token_transfer_address_index",
    ERC20TokenTransfer.token_address,
)
