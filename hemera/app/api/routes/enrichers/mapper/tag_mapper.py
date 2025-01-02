#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time  2025/1/3 17:26
# @Author  ideal93
# @File  tag_mapper.py
# @Brief
from typing import Dict, Set

from sqlmodel import select

from hemera.app.api.routes.enrichers import BaseMapper
from hemera.common.models.address import AddressIndexStats


class TagMapper(BaseMapper[bytes, str]):
    """Tag information mapper"""

    def fetch_data(self, addresses: Set[bytes]) -> Dict[bytes, str]:
        if not addresses:
            return {}
        address_stats = self.session.exec(
            select(AddressIndexStats.address, AddressIndexStats.tag).where(AddressIndexStats.address.in_(addresses))
        ).all()
        return {tag.address: tag.tag for tag in address_stats}
