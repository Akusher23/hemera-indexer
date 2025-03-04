from datetime import datetime
from typing import Any, Dict, Optional

from sqlalchemy.dialects.postgresql import JSONB
from sqlmodel import Column, Field

from hemera.common.models import HemeraModel


class SyncRecords(HemeraModel, table=True):
    __tablename__ = "sync_records"

    # Primary key
    mission_sign: str = Field(primary_key=True)

    # Fields
    last_block_number: Optional[int] = Field(default=None)
    update_time: Optional[datetime] = Field(default_factory=datetime.utcnow)

    __query_order__ = [update_time]


class FixRecords(HemeraModel, table=True):
    __tablename__ = "fix_records"

    # Primary key
    job_id: str = Field(primary_key=True)

    # Fields
    last_fixed_block_number: Optional[int] = Field(default=None)
    update_time: Optional[datetime] = Field(default_factory=datetime.utcnow)

    __query_order__ = [update_time]


class ExceptionRecords(HemeraModel, table=True):
    __tablename__ = "exception_records"

    # Primary key
    id: str = Field(primary_key=True)

    # Fields
    block_number: int = Field(default=None)
    dataclass: str = Field(default=None)
    level: str = Field(default=None)
    message_type: str = Field(default=None)
    message: str = Field(default=None)
    exception_env: Optional[Dict[str, Any]] = Field(default=None, sa_column=Column(JSONB))
    record_time: datetime = Field(default=None)

    __query_order__ = [record_time]


class FailureRecords(HemeraModel, table=True):
    __tablename__ = "failure_records"

    # Primary key
    record_id: int = Field(primary_key=True)

    # Fields
    mission_sign: str = Field(default=None)
    output_types: str = Field(default=None)
    start_block_number: int = Field(default=None)
    end_block_number: int = Field(default=None)
    exception_stage: str = Field(default=None)
    exception: Optional[Dict[str, Any]] = Field(default=None, sa_column=Column(JSONB))
    crash_time: datetime = Field(default=None)

    __query_order__ = [crash_time]
