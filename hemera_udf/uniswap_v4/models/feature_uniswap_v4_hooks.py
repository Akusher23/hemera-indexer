from sqlalchemy import Column, PrimaryKeyConstraint, func
from sqlalchemy.dialects.postgresql import BIGINT, BYTEA, TIMESTAMP, TEXT

from hemera.common.models import HemeraModel, general_converter
from hemera_udf.uniswap_v4.domains.feature_uniswap_v4 import UniswapV4Hook


class UniswapV4Hooks(HemeraModel):
    __tablename__ = "af_uniswap_v4_hooks"
    hook_address = Column(BYTEA, primary_key=True)
    pool_address = Column(BYTEA, primary_key=True)

    factory_address = Column(BYTEA)
    hook_type = Column(TEXT)  # e.g., "fee", "dynamic_fee", "limit_order", etc.
    hook_data = Column(TEXT)  # JSON string of hook-specific data

    block_number = Column(BIGINT)
    block_timestamp = Column(TIMESTAMP)

    create_time = Column(TIMESTAMP, server_default=func.now())
    update_time = Column(TIMESTAMP, server_default=func.now())

    __table_args__ = (PrimaryKeyConstraint("hook_address", "pool_address"),)

    @staticmethod
    def model_domain_mapping():
        return [
            {
                "domain": UniswapV4Hook,
                "conflict_do_update": True,
                "update_strategy": None,
                "converter": general_converter,
            },
        ] 