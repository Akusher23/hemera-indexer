#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time  2025/1/3 17:36
# @Author  ideal93
# @File  token_transfer_formatter.py
# @Brief
from typing import Any, Dict

from hemera.app.api.routes.enrichers import BaseFormatter
from hemera.app.api.routes.helper.format import format_coin_value
from hemera.app.api.routes.helper.token_transfers import TokenTransfer
from hemera.common.utils.format_utils import bytes_to_hex_str


class TokenTransferFormatter(BaseFormatter[TokenTransfer, Dict]):
    def format(self, transfer: TokenTransfer, context: Dict[str, Any]) -> Dict:
        token_map = context.get("token_map", {})
        address_map = context.get("address_map", {})

        # Base data conversion
        base_data = {
            key: bytes_to_hex_str(value) if isinstance(value, bytes) else value
            for key, value in transfer.dict().items()
        }

        # Token information
        token = token_map.get(transfer.token_address)
        if token:
            base_data["token"] = {
                "name": token.name,
                "symbol": token.symbol,
                "decimals": token.decimals,
                "logo_url": token.icon_url,
            }
            base_data["value"] = format_coin_value(transfer.value, token.decimals)
        else:
            base_data["token"] = {"name": None, "symbol": None, "decimals": None, "logo_url": None}
            base_data["value"] = format_coin_value(transfer.value, 18)

        # Address information
        base_data.update(
            {"from_display": address_map.get(transfer.from_address), "to_display": address_map.get(transfer.to_address)}
        )

        return base_data
