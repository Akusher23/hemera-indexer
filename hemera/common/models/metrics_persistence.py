from sqlalchemy import Column, func
from sqlalchemy.dialects.postgresql import JSONB, TIMESTAMP, VARCHAR

from hemera.common.models import HemeraModel


class MetricsPersistence(HemeraModel):
    __tablename__ = "metrics_persistence"

    instance = Column(VARCHAR, primary_key=True)
    value = Column(JSONB)

    create_time = Column(TIMESTAMP, server_default=func.now())
    update_time = Column(TIMESTAMP, server_default=func.now())
