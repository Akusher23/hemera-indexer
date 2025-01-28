#!/usr/bin/env python
# -*- coding: utf-8 -*-

from dataclasses import dataclass

from hemera.indexer.domains import Domain


@dataclass
class OmegaAccount(Domain):
    owner: str
    account: str
    block_number: int
    block_timestamp: int


@dataclass
class OmegaEvent(Domain):
    address: str
    event_type: str
    asset_address: str
    amount: int
    receiver_address: str
    block_number: int
    block_timestamp: int
    transaction_hash: str
    log_index: int


@dataclass
class OmegaAccountEvent(Domain):
    owner: str
    account: str
    event_type: str
    asset_address: str
    amount: int
    receiver_address: str
    block_number: int
    block_timestamp: int
    transaction_hash: str
    log_index: int


@dataclass
class OmegaAccountStrategyActivate(Domain):
    owner: str
    account: str
    strategy: str
    is_active: bool
    borrow_amount: int
    block_number: int
    block_timestamp: int


@dataclass
class OmegaAccountStrategyDeactivate(Domain):
    owner: str
    account: str
    strategy: str
    is_active: bool
    repay_amount: int
    block_number: int
    block_timestamp: int


@dataclass
class OmegaStrategyDeposit(Domain):
    account: str
    strategy: str
    amount: int
    borrow_amount: int
    shares: int
    block_number: int
    block_timestamp: int
    transaction_hash: str
    log_index: int


@dataclass
class OmegaStrategyWithdraw(Domain):
    account: str
    strategy: str
    amount: int
    repay_amount: int
    shares: int
    block_number: int
    block_timestamp: int
    transaction_hash: str
    log_index: int
