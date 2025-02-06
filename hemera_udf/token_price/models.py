from sqlalchemy import BOOLEAN, INTEGER, Column, PrimaryKeyConstraint, func
from sqlalchemy.dialects.postgresql import BIGINT, BYTEA, NUMERIC, TIMESTAMP, VARCHAR

from hemera.common.models import HemeraModel, general_converter
from hemera_udf.token_price.domains import DexBlockTokenPrice, DexBlockTokenPriceCurrent, UniswapFilteredSwapEvent


class AfDexBlockTokenPrice(HemeraModel):
    __tablename__ = "af_dex_block_token_price_20250226"

    token_address = Column(BYTEA, primary_key=True)
    block_number = Column(BIGINT, primary_key=True)

    token_symbol = Column(VARCHAR)
    decimals = Column(BIGINT)

    amount = Column(NUMERIC)
    amount_usd = Column(NUMERIC)

    token_price = Column(NUMERIC)
    block_timestamp = Column(TIMESTAMP, primary_key=True)

    create_time = Column(TIMESTAMP, server_default=func.now())
    update_time = Column(TIMESTAMP, server_default=func.now())

    __table_args__ = (PrimaryKeyConstraint("token_address", "block_number", "block_timestamp"),)

    @staticmethod
    def model_domain_mapping():
        return [
            {
                "domain": DexBlockTokenPrice,
                "conflict_do_update": True,
                "update_strategy": None,
                "converter": general_converter,
            }
        ]


class AfDexBlockTokenPriceCurrent(HemeraModel):
    __tablename__ = "af_dex_block_token_price_current_20250226"

    token_address = Column(BYTEA, primary_key=True)
    block_number = Column(BIGINT)

    token_symbol = Column(VARCHAR)
    decimals = Column(BIGINT)

    amount = Column(NUMERIC)
    amount_usd = Column(NUMERIC)

    token_price = Column(NUMERIC)
    block_timestamp = Column(TIMESTAMP)

    create_time = Column(TIMESTAMP, server_default=func.now())
    update_time = Column(TIMESTAMP, server_default=func.now())

    __table_args__ = (PrimaryKeyConstraint("token_address"),)

    @staticmethod
    def model_domain_mapping():
        return [
            {
                "domain": DexBlockTokenPriceCurrent,
                "conflict_do_update": True,
                "update_strategy": "EXCLUDED.block_number > af_dex_block_token_price_current_20250226.block_number",
                "converter": general_converter,
            }
        ]


class UniswapFilteredSwapEventModel(HemeraModel):
    __tablename__ = "af_uniswap_filtered_swap_event_20250226"
    pool_address = Column(BYTEA, primary_key=True)
    transaction_hash = Column(BYTEA, primary_key=True)
    log_index = Column(INTEGER, primary_key=True)
    block_number = Column(BIGINT)
    block_timestamp = Column(TIMESTAMP, primary_key=True)
    position_token_address = Column(BYTEA)
    transaction_from_address = Column(BYTEA)

    amount0 = Column(NUMERIC(100))
    amount1 = Column(NUMERIC(100))
    token0_price = Column(NUMERIC)
    token1_price = Column(NUMERIC)
    amount_usd = Column(NUMERIC)

    token0_address = Column(BYTEA)
    token1_address = Column(BYTEA)

    stable_token_symbol = Column(VARCHAR)

    stable_balance = Column(NUMERIC)
    token0_market_cap = Column(NUMERIC)
    token1_market_cap = Column(NUMERIC)

    create_time = Column(TIMESTAMP, server_default=func.now())
    update_time = Column(TIMESTAMP, server_default=func.now())

    __table_args__ = (PrimaryKeyConstraint("pool_address", "transaction_hash", "log_index", "block_timestamp"),)

    @staticmethod
    def model_domain_mapping():
        return [
            {
                "domain": UniswapFilteredSwapEvent,
                "conflict_do_update": True,
                "update_strategy": None,
                "converter": general_converter,
            }
        ]
