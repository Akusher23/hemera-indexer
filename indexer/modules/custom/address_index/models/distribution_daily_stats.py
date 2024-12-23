from sqlalchemy import VARCHAR, Column, Date, Double, Index, Numeric, PrimaryKeyConstraint, desc, func
from sqlalchemy.dialects.postgresql import BYTEA, INTEGER, NUMERIC, SMALLINT, TEXT, TIMESTAMP

from common.models import HemeraModel, general_converter
from indexer.modules.custom.address_index.domain import AddressTransaction


class AFDistributionDailyStats(HemeraModel):
    __tablename__ = "af_distribution_daily_stats"

    distribution_name = Column(VARCHAR, nullable=False)
    block_date = Column(Date, nullable=False)
    x = Column(Numeric, nullable=False)
    value = Column(Numeric)
    percentage = Column(Double)
    total_value = Column(Numeric)

    __table_args__ = (PrimaryKeyConstraint("distribution_name", "block_date", "x"),)
