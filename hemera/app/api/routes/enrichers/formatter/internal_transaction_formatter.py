#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time  2025/1/3 19:00
# @Author  ideal93
# @File  internal_transaction_formatter.py
# @Brief
from typing import Any, Dict

from hemera.app.api.routes.enrichers import BaseFormatter
from hemera.app.api.routes.helper.decorator import AddressExtraInfo
from hemera.app.api.routes.helper.internal_transaction import InternalTransactionAbbr
from hemera.common.utils.format_utils import bytes_to_hex_str


class InternalTransactionFormatter(BaseFormatter[InternalTransactionAbbr, Dict]):
    def format(self, transactions: InternalTransactionAbbr, context: Dict[str, Any]) -> Dict:
        tags_map = context.get("tags_map", {})
        contract_map = context.get("contract_map", {})
        ens_map = context.get("ens_map", {})

        # Base data conversion
        base_data = {
            key: bytes_to_hex_str(value) if isinstance(value, bytes) else value
            for key, value in transactions.model_dump().items()
        }

        from_address_extra_info = AddressExtraInfo(
            address=transactions.from_address,
            is_contract=transactions.from_address in contract_map,
            ens_name=ens_map.get(transactions.from_address),
            tags=tags_map.get(transactions.from_address),
            display_name=(
                ens_map.get(transactions.from_address) or contract_map.get(transactions.from_address).name
                if transactions.from_address in contract_map
                else None
            ),
        )

        to_address_extra_info = AddressExtraInfo(
            address=transactions.to_address,
            is_contract=transactions.to_address in contract_map,
            ens_name=ens_map.get(transactions.to_address),
            tags=tags_map.get(transactions.to_address),
            display_name=(
                ens_map.get(transactions.to_address) or contract_map.get(transactions.to_address).name
                if transactions.to_address in contract_map
                else None
            ),
        )

        base_data = {
            "from_address_extra_info": from_address_extra_info.dict(),
            "to_address_extra_info": to_address_extra_info.dict(),
            **base_data,
        }

        return base_data
