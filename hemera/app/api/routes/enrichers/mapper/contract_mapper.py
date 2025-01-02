#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time  2025/1/3 17:26
# @Author  ideal93
# @File  contract_mapper.py
# @Brief
from typing import Dict, List, Set

from sqlmodel import select

from hemera.app.api.routes.enrichers import BaseMapper
from hemera.common.models.contracts import Contracts
from hemera.common.utils.format_utils import bytes_to_hex_str, hex_str_to_bytes


class ContractMapper(BaseMapper[str, Contracts]):
    """Contract information mapper"""

    def fetch_data(self, addresses: List[str]) -> Dict[str, Contracts]:
        bytes_addresses = set()
        for address in addresses:
            if isinstance(address, str):
                bytes_addresses.add(hex_str_to_bytes(address))
            else:
                bytes_addresses.add(address)
        if not addresses:
            return {}
        contracts = self.session.exec(select(Contracts).where(Contracts.address.in_(bytes_addresses))).all()
        return {bytes_to_hex_str(contract.address): contract for contract in contracts}
