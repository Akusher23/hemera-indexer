from datetime import datetime
from typing import Optional

from sqlmodel import Field

from hemera.common.models import HemeraModel


class DailyTokensStats(HemeraModel, table=True):
    __tablename__ = "af_stats_na_daily_tokens"

    # Primary key
    block_date: datetime = Field(primary_key=True)

    # Fields
    erc20_active_address_cnt: Optional[int] = Field(default=None)
    erc20_total_transfer_cnt: Optional[int] = Field(default=None)
    erc721_active_address_cnt: Optional[int] = Field(default=None)
    erc721_total_transfer_cnt: Optional[int] = Field(default=None)
    erc1155_active_address_cnt: Optional[int] = Field(default=None)
    erc1155_total_transfer_cnt: Optional[int] = Field(default=None)
