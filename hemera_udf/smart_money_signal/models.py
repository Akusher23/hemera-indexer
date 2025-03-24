from sqlalchemy import INTEGER, Column, func
from sqlalchemy.dialects.postgresql import BIGINT, BYTEA, NUMERIC, TIMESTAMP

from hemera.common.models import HemeraModel, general_converter
from hemera_udf.smart_money_signal.domains import SmartMoneySignalTrade


class SmartMoneySignalMetricsModel(HemeraModel):
    __tablename__ = "af_smart_money_signal_trades"

    trader_id = Column(BYTEA, primary_key=True)
    token_address = Column(BYTEA, primary_key=True)
    pool_address = Column(BYTEA, primary_key=True)
    block_number = Column(BIGINT, primary_key=True)
    block_timestamp = Column(TIMESTAMP, primary_key=True)
    transaction_hash = Column(BYTEA, primary_key=True)
    log_index = Column(INTEGER, primary_key=True)
    token_price = Column(NUMERIC)

    swap_in_amount = Column(NUMERIC)
    swap_in_amount_usd = Column(NUMERIC)
    swap_out_amount = Column(NUMERIC)
    swap_out_amount_usd = Column(NUMERIC)
    transfer_in_amount = Column(NUMERIC)
    transfer_in_amount_usd = Column(NUMERIC)
    transfer_out_amount = Column(NUMERIC)
    transfer_out_amount_usd = Column(NUMERIC)

    swap_in_count = Column(BIGINT)  # 交换输入次数
    swap_out_count = Column(BIGINT)  # 交换输出次数
    transfer_in_count = Column(BIGINT)  # 转账输入次数
    transfer_out_count = Column(BIGINT)  # 转账输出次数

    create_time = Column(TIMESTAMP, server_default=func.now())
    update_time = Column(TIMESTAMP, server_default=func.now())

    @staticmethod
    def model_domain_mapping():
        return [
            {
                "domain": SmartMoneySignalTrade,
                "conflict_do_update": False,
                "update_strategy": None,
                "converter": general_converter,
            }
        ]
