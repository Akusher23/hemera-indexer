#!/usr/bin/env python
# -*- coding: utf-8 -*-

from dataclasses import dataclass

from hemera.indexer.domains import Domain


@dataclass
class OmegaAccount(Domain):
    onwer: str
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

@dataclass
class OmegaAccountEvent(Domain):
    onwer: str
    account: str
    event_type: str
    asset_address: str
    amount: int
    receiver_address: str
    block_number: int
    block_timestamp: int
