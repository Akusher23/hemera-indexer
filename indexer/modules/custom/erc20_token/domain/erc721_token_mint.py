#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from dataclasses import dataclass

from indexer.domain import FilterData


@dataclass
class ERC20TokenTransfer(FilterData):
    address: str
    token_address: str
    value: int
    block_number: int
    block_timestamp: int
    transaction_hash: str
    log_index: int
