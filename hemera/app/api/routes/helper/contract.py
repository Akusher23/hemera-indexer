#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time  2024/12/25 14:12
# @Author  ideal93
# @File  contract.py
# @Brief

from typing import List, Optional, Union

from sqlmodel import Session

from hemera.app.api.routes.helper import ColumnType, process_columns
from hemera.common.models.contracts import Contracts
from hemera.common.utils.format_utils import hex_str_to_bytes


def _process_columns(columns: ColumnType):
    return process_columns(Contracts, columns)


def get_contract_by_address(session: Session, address: str, columns: ColumnType = "*") -> Optional[Contracts]:
    """Get contract by its address

    Args:
        session: SQLModel session
        address: Contract address (hex string)
        columns: Can be "*" for all columns, single column name, or list of column names
                Examples:
                - "*": select all columns
                - "address": select only address column
                - "address,bytecode": select address and bytecode columns
                - ["address", "bytecode"]: select address and bytecode columns

    Returns:
        Optional[Contracts]: Matching contract or None
        When specific columns are selected, other attributes will raise AttributeError when accessed

    Raises:
        ValueError: If address format is invalid
    """
    bytes_address = hex_str_to_bytes(address)
    statement = _process_columns(columns)
    statement = statement.where(Contracts.address == bytes_address)
    return session.exec(statement).first()


def get_contracts_by_addresses(
    session: Session, addresses: List[Union[str, bytes]], columns: ColumnType = "*"
) -> List[Contracts]:
    """Get multiple contracts by their addresses

    Args:
        session: SQLModel session
        addresses: List of contract addresses (can be hex strings or bytes)
        columns: Can be "*" for all columns, single column name, or list of column names
                Examples:
                - "*": select all columns
                - "address": select only address column
                - "address,bytecode": select address and bytecode columns
                - ["address", "bytecode"]: select address and bytecode columns

    Returns:
        List[Contracts]: List of matching contracts
        When specific columns are selected, other attributes will raise AttributeError when accessed

    Raises:
        ValueError: If any hex string address format is invalid
    """
    # Convert addresses to bytes if needed
    bytes_addresses = {addr if isinstance(addr, bytes) else hex_str_to_bytes(addr) for addr in addresses}

    statement = _process_columns(columns)
    statement = statement.where(Contracts.address.in_(bytes_addresses))
    return session.exec(statement).all()
