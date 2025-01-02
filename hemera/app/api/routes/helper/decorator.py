from decimal import Decimal
from typing import Any, Dict, List, Optional

from pydantic import BaseModel
from sqlmodel import Session

from hemera.app.api.routes.helper.address import get_address_display_mapping
from hemera.app.api.routes.helper.contract import get_contracts_by_addresses
from hemera.common.utils.format_utils import format_to_dict


class AddressExtraInfo(BaseModel):
    """Standardized response model for address transactions"""

    address: str
    is_contract: Optional[bool]
    ens_name: Optional[str]
    tags: Optional[List[str]]
    display_name: Optional[str]


def fill_address_display_to_logs(
    session: Session, log_list: List[Dict[str, Any]], all_address_list: Optional[List[bytes]] = None
) -> None:
    """Fill address display names to log entries

    Args:
        session: SQLModel session
        log_list: List of log dictionaries to enrich
        all_address_list: Optional pre-collected list of addresses in bytes format
    """
    if not all_address_list:
        all_address_list = []

    # Collect addresses from logs
    for log in log_list:
        all_address_list.append(log["address"])

    # Get address display mapping

    # Fill display names
    for log in log_list:
        if log["address"] in address_map:
            log["address_display_name"] = address_map[log["address"]]


def fill_is_contract_to_transactions(session: Session, transaction_list: List[Dict[str, Any]]) -> None:
    """Fill contract flags to transaction entries

    Args:
        session: SQLModel session
        transaction_list: List of transaction dictionaries to enrich
    """
    addresses = []
    for tx in transaction_list:
        addresses.append(tx["from_address"])
        addresses.append(tx["to_address"])

    # Get contracts for addresses
    contracts = get_contracts_by_addresses(session, addresses, columns=["address"])
    contract_set = set(contract.address for contract in contracts)

    # Fill contract flags
    for tx in transaction_list:
        tx["to_address_is_contract"] = tx["to_address"] in contract_set
        tx["from_address_is_contract"] = tx["from_address"] in contract_set


def fill_address_display_to_transactions(session: Session, transaction_list: List[Dict[str, Any]]) -> None:
    """Fill address display names to transaction entries

    Args:
        session: SQLModel session
        transaction_list: List of transaction dictionaries to enrich
        bytea_address_list: Optional pre-collected list of addresses in bytes format
    """
    addresses = []
    for tx in transaction_list:
        addresses.append(tx["from_address"])
        addresses.append(tx["to_address"])

    # Get address display mapping
    address_map = get_address_display_mapping(session, addresses)

    # Fill display names
    for tx in transaction_list:
        from_address = tx["from_address"]
        to_address = tx["to_address"]

        tx["from_address_display_name"] = address_map[from_address] if from_address in address_map else from_address
        tx["to_address_display_name"] = address_map[to_address] if to_address in address_map else to_address


def process_token_transfer(token_transfers: List[Any], token_type: str) -> List[Dict[str, Any]]:
    """Process and format token transfer data

    Args:
        token_transfers: List of token transfer objects to process
        token_type: Type of token transfer ("tokentxns", "tokentxns-nft", "tokentxns-nft1155")

    Returns:
        List of processed token transfer dictionaries
    """
    token_transfer_list = []

    for transfer in token_transfers:
        # Convert transfer object to dictionary
        transfer_data = format_to_dict(transfer)

        # Add basic token information
        transfer_data["type"] = token_type
        transfer_data["token_symbol"] = transfer.symbol or "UNKNOWN"
        transfer_data["token_name"] = transfer.name or "Unknown Token"

        # Process based on token type
        if token_type == "tokentxns":
            # Handle fungible token transfers (ERC20)
            decimals = transfer.decimals or 18
            value = Decimal(transfer.value) / Decimal(10**decimals)
            transfer_data["value"] = f"{value:f}".rstrip("0").rstrip(".")
            transfer_data["token_logo_url"] = transfer.icon_url or None
        else:
            # Handle NFT transfers (ERC721 and ERC1155)
            transfer_data["token_id"] = f"{transfer.token_id:f}"
            transfer_data["token_logo_url"] = None

            if token_type == "tokentxns-nft1155":
                transfer_data["value"] = f"{transfer.value:f}"

        token_transfer_list.append(transfer_data)

    return token_transfer_list
