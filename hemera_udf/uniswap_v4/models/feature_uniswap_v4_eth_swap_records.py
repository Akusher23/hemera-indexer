from sqlalchemy import Column, PrimaryKeyConstraint, func, Boolean
from sqlalchemy.dialects.postgresql import BIGINT, BYTEA, NUMERIC, TIMESTAMP

from hemera.common.models import HemeraModel, general_converter
from hemera_udf.uniswap_v4.domains.feature_uniswap_v4 import UniswapV4ETHSwapEvent


class UniswapV4ETHSwapRecords(HemeraModel):
    __tablename__ = "af_uniswap_v4_eth_swap_records"
    pool_address = Column(BYTEA, primary_key=True)
    transaction_hash = Column(BYTEA, primary_key=True)
    log_index = Column(BIGINT, primary_key=True)

    eth_amount = Column(NUMERIC(100))
    token_address = Column(BYTEA)
    token_amount = Column(NUMERIC(100))
    wallet_address = Column(BYTEA)
    hook_address = Column(BYTEA)
    is_eth_to_token = Column(Boolean)

    block_number = Column(BIGINT)
    block_timestamp = Column(TIMESTAMP)

    create_time = Column(TIMESTAMP, server_default=func.now())
    update_time = Column(TIMESTAMP, server_default=func.now())

    __table_args__ = (PrimaryKeyConstraint("pool_address", "transaction_hash", "log_index"),)

    @staticmethod
    def model_domain_mapping():
        return [
            {
                "domain": UniswapV4ETHSwapEvent,
                "conflict_do_update": True,
                "update_strategy": None,
                "converter": general_converter,
            },
        ] 