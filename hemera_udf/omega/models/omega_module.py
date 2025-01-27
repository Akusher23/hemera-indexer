#!/usr/bin/env python
# -*- coding: utf-8 -*-

from sqlalchemy import Column, Index, PrimaryKeyConstraint, func, text
from sqlalchemy.dialects.postgresql import BIGINT, BOOLEAN, BYTEA, INTEGER, NUMERIC, TIMESTAMP, VARCHAR

from hemera.common.models import HemeraModel, general_converter
from hemera_udf.omega.domains import (
    OmegaAccount,
    OmegaAccountEvent,
    OmegaAccountStrategyActivate,
    OmegaAccountStrategyDeactivate,
    OmegaEvent,
    OmegaStrategyDeposit,
    OmegaStrategyWithdraw,
)


class OmegaAccounts(HemeraModel):
    __tablename__ = "af_omega_accounts"

    owner = Column(BYTEA, primary_key=True)
    account = Column(BYTEA, primary_key=True)

    block_number = Column(BIGINT)
    block_timestamp = Column(TIMESTAMP)

    create_time = Column(TIMESTAMP, server_default=func.now())
    update_time = Column(TIMESTAMP, server_default=func.now())

    @staticmethod
    def model_domain_mapping():
        return [
            {
                "domain": OmegaAccount,
                "conflict_do_update": True,
                "update_strategy": "EXCLUDED.block_number > af_omega_accounts.block_number",
                "converter": general_converter,
            },
        ]


class OmegaAccountStrategies(HemeraModel):
    __tablename__ = "af_omega_account_strategies"

    owner = Column(BYTEA, primary_key=True)
    account = Column(BYTEA, primary_key=True)
    strategy = Column(BYTEA, primary_key=True)

    is_active = Column(BOOLEAN, server_default=text("false"))
    borrow_amount = Column(NUMERIC(100))
    repay_amount = Column(NUMERIC(100))

    block_number = Column(BIGINT)
    block_timestamp = Column(TIMESTAMP)

    create_time = Column(TIMESTAMP, server_default=func.now())
    update_time = Column(TIMESTAMP, server_default=func.now())

    @staticmethod
    def model_domain_mapping():
        return [
            {
                "domain": OmegaAccountStrategyActivate,
                "conflict_do_update": True,
                "update_strategy": "EXCLUDED.block_number > af_omega_account_strategies.block_number",
                "converter": general_converter,
            },
            {
                "domain": OmegaAccountStrategyDeactivate,
                "conflict_do_update": True,
                "update_strategy": "EXCLUDED.block_number > af_omega_account_strategies.block_number",
                "converter": general_converter,
            },
        ]


class OmegaStrategyEvents(HemeraModel):
    __tablename__ = "af_omega_strategy_events"

    strategy = Column(BYTEA, primary_key=True)
    amount = Column(NUMERIC(100))
    borrow_amount = Column(NUMERIC(100))
    repay_amount = Column(NUMERIC(100))
    shares = Column(NUMERIC(100))

    transaction_hash = Column(BYTEA, primary_key=True)
    log_index = Column(INTEGER, primary_key=True)

    block_number = Column(BIGINT, primary_key=True)
    block_timestamp = Column(TIMESTAMP)

    create_time = Column(TIMESTAMP, server_default=func.now())
    update_time = Column(TIMESTAMP, server_default=func.now())

    @staticmethod
    def model_domain_mapping():
        return [
            {
                "domain": OmegaStrategyDeposit,
                "conflict_do_update": True,
                "update_strategy": None,
                "converter": general_converter,
            },
            {
                "domain": OmegaStrategyWithdraw,
                "conflict_do_update": True,
                "update_strategy": None,
                "converter": general_converter,
            },
        ]


class OmegaAccountEvents(HemeraModel):
    __tablename__ = "af_omega_account_events"

    transaction_hash = Column(BYTEA, primary_key=True)
    log_index = Column(INTEGER, primary_key=True)

    owner = Column(BYTEA)
    account = Column(BYTEA)
    event_type = Column(VARCHAR)
    asset_address = Column(BYTEA)
    amount = Column(NUMERIC(100))
    receiver_address = Column(BYTEA)

    block_number = Column(BIGINT, primary_key=True)
    block_timestamp = Column(TIMESTAMP)

    create_time = Column(TIMESTAMP, server_default=func.now())
    update_time = Column(TIMESTAMP, server_default=func.now())

    # If the custom job supports reorg,
    # the data table that stores the data must contain a reorg field.
    reorg = Column(BOOLEAN, server_default=text("false"))

    __table_args__ = (PrimaryKeyConstraint("block_number", "log_index", "transaction_hash"),)

    @staticmethod
    def model_domain_mapping():
        return [
            {
                "domain": OmegaAccountEvent,
                "conflict_do_update": False,
                "update_strategy": None,
                "converter": general_converter,
            }
        ]


class OmegaEvents(HemeraModel):
    __tablename__ = "af_omega_events"

    transaction_hash = Column(BYTEA, primary_key=True)
    log_index = Column(INTEGER, primary_key=True)

    address = Column(BYTEA)
    event_type = Column(VARCHAR)
    asset_address = Column(BYTEA)
    amount = Column(NUMERIC(100))
    receiver_address = Column(BYTEA)

    block_number = Column(BIGINT, primary_key=True)
    block_timestamp = Column(TIMESTAMP)

    create_time = Column(TIMESTAMP, server_default=func.now())
    update_time = Column(TIMESTAMP, server_default=func.now())

    # If the custom job supports reorg,
    # the data table that stores the data must contain a reorg field.
    reorg = Column(BOOLEAN, server_default=text("false"))

    __table_args__ = (PrimaryKeyConstraint("block_number", "log_index", "transaction_hash"),)

    @staticmethod
    def model_domain_mapping():
        return [
            {
                "domain": OmegaEvent,
                "conflict_do_update": False,
                "update_strategy": None,
                "converter": general_converter,
            }
        ]


Index("af_omega_account_events_address_index", OmegaAccountEvents.owner)
Index("af_omega_events_address_index", OmegaEvents.address)
