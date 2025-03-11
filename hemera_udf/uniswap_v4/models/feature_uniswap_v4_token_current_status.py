from sqlalchemy import Column, PrimaryKeyConstraint, func
from sqlalchemy.dialects.postgresql import BIGINT, BYTEA, NUMERIC, TIMESTAMP

from hemera.common.models import HemeraModel, general_converter
from hemera_udf.uniswap_v4.domains.feature_uniswap_v4 import UniswapV4TokenCurrentStatus


class UniswapV4TokenCurrentStatuses(HemeraModel):
    __tablename__ = "af_uniswap_v4_token_current_statuses"
    position_token_address = Column(BYTEA, primary_key=True)
    token_id = Column(NUMERIC(100), primary_key=True)

    pool_address = Column(BYTEA)
    wallet_address = Column(BYTEA)
    liquidity = Column(NUMERIC(100))

    block_number = Column(BIGINT)
    block_timestamp = Column(TIMESTAMP)

    create_time = Column(TIMESTAMP, server_default=func.now())
    update_time = Column(TIMESTAMP, server_default=func.now())

    __table_args__ = (PrimaryKeyConstraint("position_token_address", "token_id"),)

    @staticmethod
    def model_domain_mapping():
        return [
            {
                "domain": UniswapV4TokenCurrentStatus,
                "conflict_do_update": True,
                "update_strategy": None,
                "converter": general_converter,
            },
        ] 