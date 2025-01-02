#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time  2025/1/3 17:26
# @Author  ideal93
# @File  token_mapper.py
# @Brief
from typing import Dict, Set

from sqlmodel import select

from hemera.app.api.routes.enrichers import BaseMapper
from hemera.common.models.tokens import Tokens


class TokenMapper(BaseMapper[bytes, Tokens]):
    """Token information mapper"""

    def fetch_data(self, addresses: Set[bytes]) -> Dict[bytes, Tokens]:
        if not addresses:
            return {}
        tokens = self.session.exec(select(Tokens).where(Tokens.address.in_(addresses))).all()
        return {token.address: token for token in tokens}
