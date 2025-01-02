#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time  2025/1/3 19:06
# @Author  ideal93
# @File  transaction_formatter.py.py
# @Brief
from typing import Any, Dict

from hemera.app.api.routes.enrichers import BaseFormatter
from hemera.app.api.routes.helper.format import format_coin_value
from hemera.app.api.routes.helper.transaction import TransactionAbbr
from hemera.common.utils.format_utils import bytes_to_hex_str


class TransactionFormatter(BaseFormatter[TransactionAbbr, Dict]):
    def format(self, transactions: TransactionAbbr, context: Dict[str, Any]) -> Dict:
        token_map = context.get("token_map", {})
        address_map = context.get("address_map", {})

        # Base data conversion
        base_data = {
            key: bytes_to_hex_str(value) if isinstance(value, bytes) else value
            for key, value in transactions.dict().items()
        }

        # Token information
        token = token_map.get(transactions.token_address)
        if token:
            base_data["token"] = {
                "name": token.name,
                "symbol": token.symbol,
                "decimals": token.decimals,
                "logo_url": token.icon_url,
            }
            base_data["value"] = format_coin_value(transactions.value, token.decimals)
        else:
            base_data["token"] = {"name": None, "symbol": None, "decimals": None, "logo_url": None}
            base_data["value"] = format_coin_value(transactions.value, 18)

        # Address information
        base_data.update(
            {
                "from_display": address_map.get(transactions.from_address),
                "to_display": address_map.get(transactions.to_address),
            }
        )

        return base_data
