#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time  2024/12/22
# @Author  ideal93
# @File  address_utils.py
# @Brief

from enum import Enum
from typing import Dict, List, Optional, Union

from sqlmodel import Session, select

from hemera.app.api.routes.helper import ColumnType
from hemera.app.core.service import extra_contract_service, extra_ens_service
from hemera.common.models.address import AddressIndexStats
from hemera.common.models.contracts import Contracts
from hemera.common.models.tokens import Tokens
from hemera.common.utils.format_utils import bytes_to_hex_str, hex_str_to_bytes

# Type definitions


class TokenTransferType(str, Enum):
    """Token transfer types for address statistics"""

    ERC20 = "erc20"
    ERC721 = "erc721"
    ERC1155 = "erc1155"
    TOKEN_TXNS = "tokentxns"
    TOKEN_TXNS_NFT = "tokentxns-nft"
    TOKEN_TXNS_NFT1155 = "tokentxns-nft1155"


# Mapping from token type to stats column
TOKEN_TYPE_TO_COLUMN = {
    TokenTransferType.TOKEN_TXNS: AddressIndexStats.erc20_transfer_count,
    TokenTransferType.TOKEN_TXNS_NFT: AddressIndexStats.nft_721_transfer_count,
    TokenTransferType.TOKEN_TXNS_NFT1155: AddressIndexStats.nft_1155_transfer_count,
    TokenTransferType.ERC20: AddressIndexStats.erc20_transfer_count,
    TokenTransferType.ERC721: AddressIndexStats.nft_721_transfer_count,
    TokenTransferType.ERC1155: AddressIndexStats.nft_1155_transfer_count,
}


def get_txn_cnt_by_address(session: Session, address: str) -> Optional[int]:
    bytes_address = hex_str_to_bytes(address)
    statement = select(AddressIndexStats.transaction_count).where(AddressIndexStats.address == bytes_address)
    return session.exec(statement).first()


def get_token_transfer_count(
    session: Session, address: str, token_type: Union[str, TokenTransferType], columns: ColumnType = "*"
) -> Optional[int]:
    """Get token transfer count for an address by token type

    Args:
        session: SQLModel session
        address: Address in hex string format
        token_type: Type of token transfers to count (e.g. "erc20", "erc721")
        columns: Can be "*" for all columns, single column name, or list of column names

    Returns:
        Optional[int]: Transfer count or None if not found

    Raises:
        ValueError: If address format is invalid or token type is not supported
    """
    if isinstance(token_type, str):
        token_type = TokenTransferType(token_type.lower())

    bytes_address = hex_str_to_bytes(address)
    statement = select(TOKEN_TYPE_TO_COLUMN[token_type]).where(AddressIndexStats.address == bytes_address)
    return session.exec(statement).first()


def get_transaction_count(session: Session, address: str, columns: ColumnType = "*") -> Optional[int]:
    """Get total transaction count for an address

    Args:
        session: SQLModel session
        address: Address in hex string format
        columns: Can be "*" for all columns, single column name, or list of column names

    Returns:
        Optional[int]: Transaction count or None if not found

    Raises:
        ValueError: If address format is invalid
    """
    bytes_address = hex_str_to_bytes(address)
    statement = select(AddressIndexStats.transaction_count).where(AddressIndexStats.address == bytes_address)
    return session.exec(statement).first()


def get_address_display_mapping(
    session: Session, addresses: List[Union[str, bytes]], include_ens: bool = True
) -> Dict[str, str]:
    """Get display information for a list of addresses

    Args:
        session: SQLModel session
        addresses: List of addresses (can be hex strings or bytes)
        include_ens: Whether to include ENS resolution (default: True)

    Returns:
        Dict[str, str]: Mapping of addresses to their display names

    Raises:
        ValueError: If any hex string address format is invalid
    """
    if not addresses:
        return {}

    # Convert addresses to bytes format and deduplicate
    bytes_addresses = {addr if isinstance(addr, bytes) else hex_str_to_bytes(addr) for addr in addresses if addr}

    # Convert addresses to hex strings for final mapping
    str_addresses = {bytes_to_hex_str(addr) for addr in bytes_addresses}

    # Initialize result mapping
    address_map: Dict[str, str] = {}

    # Get proxy contract mappings
    statement = select(Contracts.address, Contracts.verified_implementation_contract).where(
        Contracts.address.in_(bytes_addresses), Contracts.verified_implementation_contract != None
    )
    proxy_results = session.exec(statement).all()
    proxy_mapping = {address.address: address.verified_implementation_contract for address in proxy_results}

    # Get contract names for all addresses including implementations
    all_addresses = str_addresses.union(bytes_to_hex_str(addr) for addr in proxy_mapping.values())

    if extra_contract_service:
        contract_names = extra_contract_service.get_contract_names(list(all_addresses))
        address_map.update({address["address"]: address["contract_name"] for address in contract_names})

    # Apply implementation names to proxy contracts
    for proxy_addr, impl_addr in proxy_mapping.items():
        str_proxy = bytes_to_hex_str(proxy_addr)
        str_impl = bytes_to_hex_str(impl_addr)
        if str_impl in address_map:
            address_map[str_proxy] = address_map[str_impl]

    # Get token information
    statement = select(Tokens.address, Tokens.name, Tokens.symbol).where(Tokens.address.in_(bytes_addresses))
    tokens = session.exec(statement).all()

    for token in tokens:
        str_address = bytes_to_hex_str(token.address)
        address_map[str_address] = f"{token.name}: {token.symbol} Token"

    # Get ENS names if requested
    if include_ens and extra_ens_service:
        ens_names = extra_ens_service.batch_get_address_ens(list(str_addresses))
        address_map.update(ens_names)

    # Get manual tags from address stats
    statement = select(AddressIndexStats.address, AddressIndexStats.tag).where(
        AddressIndexStats.address.in_(bytes_addresses), AddressIndexStats.tag != None
    )
    tagged_addresses = session.exec(statement).all()

    for addr in tagged_addresses:
        str_address = bytes_to_hex_str(addr.address)
        address_map[str_address] = addr.tag

    return address_map


def get_address_ens_names(session: Session, addresses: List[str]) -> Dict[str, str]:
    """Get ENS names for a list of addresses

    Args:
        session: SQLModel session
        addresses: List of addresses in hex string format

    Returns:
        Dict[str, str]: Mapping of addresses to their ENS names

    Note:
        Returns empty dict if ENS client is not configured
    """
    if not addresses:
        return {}

    if ens_client:
        return ens_client.batch_get_address_ens(addresses)
    return {}
