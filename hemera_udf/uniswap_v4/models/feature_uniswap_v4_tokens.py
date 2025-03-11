from sqlalchemy import Column, PrimaryKeyConstraint, func
from sqlalchemy.dialects.postgresql import BIGINT, BYTEA, NUMERIC, TIMESTAMP

from hemera.common.models import HemeraModel, general_converter
from hemera_udf.uniswap_v4.domains.feature_uniswap_v4 import UniswapV4Token


class UniswapV4Tokens(HemeraModel):
    __tablename__ = "af_uniswap_v4_tokens"
    position_token_address = Column(BYTEA)
    token_id = Column(NUMERIC(100))
    block_number = Column(BIGINT, primary_key=True)

    pool_address = Column(BYTEA)
    tick_lower = Column(NUMERIC(100))
    tick_upper = Column(NUMERIC(100))
    fee = Column(NUMERIC(100))

    block_timestamp = Column(TIMESTAMP)

    create_time = Column(TIMESTAMP, server_default=func.now())
    update_time = Column(TIMESTAMP, server_default=func.now())

    __table_args__ = (PrimaryKeyConstraint("position_token_address", "token_id", "block_number"),)

    @staticmethod
    def model_domain_mapping():
        return [
            {
                "domain": UniswapV4Token,
                "conflict_do_update": True,
                "update_strategy": None,
                "converter": general_converter,
            },
        ] 