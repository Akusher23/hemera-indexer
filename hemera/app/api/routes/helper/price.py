#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time  2024/12/25 14:12
# @Author  ideal93
# @File  contract.py
# @Brief
from datetime import datetime
from typing import Any, Dict, List

from sqlmodel import Session

from hemera.app.api.routes.helper.format import format_dollar_value
from hemera.app.api.routes.helper.token import get_coin_prices


def fill_price_to_transactions(session: Session, transaction_list: List[Dict[str, Any]]) -> None:
    """Fill price to transaction entries

    Args:
        session: SQLModel session
        transaction_list: List of transaction dictionaries to enrich
    """
    block_dates = set()
    for tx in transaction_list:
        block_dates.add(datetime.fromisoformat(tx["block_timestamp"]).replace(second=0, microsecond=0))

    prices = get_coin_prices(session, list(block_dates))

    price_map = {price.block_date: price.price for price in prices}
    for tx in transaction_list:
        coin_price = price_map.get(datetime.fromisoformat(tx["block_timestamp"]).replace(second=0, microsecond=0), 0.0)
        tx["transaction_fee_usd"] = format_dollar_value(coin_price * float(tx["transaction_fee"]))
        tx["value_usd"] = format_dollar_value(coin_price * float(tx["value"]))
