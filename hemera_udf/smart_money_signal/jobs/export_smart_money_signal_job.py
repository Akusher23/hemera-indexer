import logging

import requests

from hemera.indexer.jobs.base_job import ExtensionJob
from hemera_udf.smart_money_signal.domains import SmartMoneySignalTrade
from hemera_udf.swap.domains.swap_event_domain import (
    FourMemeSwapEvent,
    UniswapV2SwapEvent,
    UniswapV3SwapEvent,
    UniswapV4SwapEvent,
)

logger = logging.getLogger(__name__)


class ExportSmartMoneySignal(ExtensionJob):
    dependency_types = [UniswapV2SwapEvent, UniswapV3SwapEvent, UniswapV4SwapEvent, FourMemeSwapEvent]

    output_types = [SmartMoneySignalTrade]
    able_to_reorg = True

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._service = kwargs["config"].get("db_service")
        self.config = kwargs["config"].get("export_block_token_price_job", {})
        self.url = kwargs["config"].get("SmartMoneySignalMetrics").get("url")

        # todo: replace with provided list
        self.smart_money_address_list = set()

    def _process(self, **kwargs):
        smart_money_address_list = set(requests.get(self.url).json().get("data"))

        uniswap_v2_list = self._data_buff.get(UniswapV2SwapEvent.type())
        uniswap_v3_list = self._data_buff.get(UniswapV3SwapEvent.type())
        uniswap_v4_list = self._data_buff.get(UniswapV4SwapEvent.type())
        fourmeme_list = self._data_buff.get(FourMemeSwapEvent.type())

        all_swap_events = uniswap_v2_list + uniswap_v3_list + uniswap_v4_list + fourmeme_list

        for swap_event in all_swap_events:
            trade_id = swap_event.transaction_from_address
            if trade_id not in smart_money_address_list:
                continue

            if not swap_event.amount0 or not swap_event.amount1:
                continue

            if swap_event.token0_address not in self.config:
                token_address = swap_event.token0_address
                token_price = swap_event.token0_price

                if swap_event.amount0 < 0:
                    swap_in_amount = abs(swap_event.amount0)
                    swap_in_amount_usd = swap_event.amount_usd

                    swap_out_amount = 0
                    swap_out_amount_usd = 0

                else:

                    swap_in_amount = 0
                    swap_in_amount_usd = 0

                    swap_out_amount = abs(swap_event.amount0)
                    swap_out_amount_usd = swap_event.amount_usd

                domain = SmartMoneySignalTrade(
                    block_number=swap_event.block_number,
                    block_timestamp=swap_event.block_timestamp,
                    trader_id=trade_id,
                    token_address=token_address,
                    pool_address=swap_event.pool_address,
                    transaction_hash=swap_event.transaction_hash,
                    log_index=swap_event.log_index,
                    swap_in_amount=swap_in_amount,
                    swap_in_amount_usd=swap_in_amount_usd,
                    swap_out_amount=swap_out_amount,
                    swap_out_amount_usd=swap_out_amount_usd,
                    token_price=token_price,
                )

                if domain.token_address and domain.trader_id:
                    self._collect_domain(domain)

            # 处理 token1
            if swap_event.token1_address not in self.config:
                token_address = swap_event.token1_address
                token_price = swap_event.token1_price

                if swap_event.amount1 < 0:
                    swap_in_amount = abs(swap_event.amount1)
                    swap_in_amount_usd = swap_event.amount_usd

                    swap_out_amount = 0
                    swap_out_amount_usd = 0
                else:
                    swap_in_amount = 0
                    swap_in_amount_usd = 0

                    swap_out_amount = abs(swap_event.amount1)
                    swap_out_amount_usd = swap_event.amount_usd

                domain = SmartMoneySignalTrade(
                    block_number=swap_event.block_number,
                    block_timestamp=swap_event.block_timestamp,
                    trader_id=trade_id,
                    token_address=token_address,
                    pool_address=swap_event.pool_address,
                    transaction_hash=swap_event.transaction_hash,
                    log_index=swap_event.log_index,
                    swap_in_amount=swap_in_amount,
                    swap_in_amount_usd=swap_in_amount_usd,
                    swap_out_amount=swap_out_amount,
                    swap_out_amount_usd=swap_out_amount_usd,
                    token_price=token_price,
                )

                if domain.token_address and domain.trader_id:
                    self._collect_domain(domain)
