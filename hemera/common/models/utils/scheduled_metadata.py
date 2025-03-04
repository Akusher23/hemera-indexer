from datetime import datetime
from typing import Optional

from sqlmodel import Field

from hemera.common.models import HemeraModel, general_converter


class ScheduledMetadata(HemeraModel, table=True):
    __tablename__ = "scheduled_metadata"

    # Primary key and basic fields
    id: int = Field(primary_key=True)
    dag_id: str = Field(primary_key=True)
    execution_date: Optional[datetime] = Field(default=None)
    last_data_timestamp: Optional[datetime] = Field(default=None)
