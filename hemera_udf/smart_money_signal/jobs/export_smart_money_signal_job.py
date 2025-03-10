import logging
from collections import defaultdict

from hemera.indexer.domains.token import Token
from hemera.indexer.domains.token_transfer import ERC20TokenTransfer
from hemera.indexer.jobs.base_job import ExtensionJob
from hemera_udf.smart_money_signal.domains import SmartMoneySignalMetrics
from hemera_udf.uniswap_v2 import UniswapV2SwapEvent
from hemera_udf.uniswap_v3 import UniswapV3SwapEvent

logger = logging.getLogger(__name__)


class ExportSmartMoneySignal(ExtensionJob):
    dependency_types = [UniswapV2SwapEvent, UniswapV3SwapEvent, ERC20TokenTransfer]

    output_types = [SmartMoneySignalMetrics]
    able_to_reorg = True

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._service = kwargs["config"].get("db_service")
        self.config = kwargs["config"].get("export_block_token_price_job", {})

        # todo: replace with provided list
        self.smart_money_address_list = set()

    def _process(self, **kwargs):
        address_token_swap_dict = defaultdict(
            lambda: {
                "swap_in_amount": 0,
                "swap_in_amount_usd": 0,
                "swap_in_count": 0,
                "swap_out_amount": 0,
                "swap_out_amount_usd": 0,
                "swap_out_count": 0,
                "transfer_in_amount": 0,
                "transfer_in_amount_usd": 0,
                "transfer_in_count": 0,
                "transfer_out_amount": 0,
                "transfer_out_amount_usd": 0,
                "transfer_out_count": 0,
            }
        )

        uniswap_v2_list = self._data_buff.get(UniswapV2SwapEvent.type())
        uniswap_v3_list = self._data_buff.get(UniswapV3SwapEvent.type())

        for swap_event in uniswap_v2_list + uniswap_v3_list:
            block_timestamp = swap_event.block_timestamp
            block_number = swap_event.block_number
            trader_id = swap_event.transaction_from_address

            in_key = (block_timestamp, block_number, trader_id, swap_event.token0_address)

            address_token_swap_dict[in_key]["swap_in_amount"] += abs(swap_event.amount0)
            address_token_swap_dict[in_key]["swap_in_amount_usd"] += swap_event.amount_usd or 0
            address_token_swap_dict[in_key]["swap_in_count"] += 1

            out_key = (block_timestamp, block_number, trader_id, swap_event.token1_address)

            address_token_swap_dict[out_key]["swap_out_amount"] += abs(swap_event.amount1)
            address_token_swap_dict[out_key]["swap_out_amount_usd"] += swap_event.amount_usd or 0
            address_token_swap_dict[out_key]["swap_out_count"] += 1

        token_transfers = self._data_buff.get(ERC20TokenTransfer.type())

        for transfer in token_transfers:
            block_timestamp = transfer.block_timestamp
            block_number = transfer.block_number

            token_address = transfer.token_address
            decimals = self.tokens.get(token_address).get("decimals")

            in_key = (block_timestamp, block_number, transfer.to_address, token_address)

            address_token_swap_dict[in_key]["transfer_in_amount"] += transfer.value / 10**decimals
            # address_token_swap_dict[in_key]['transfer_in_amount_usd'] += 0
            address_token_swap_dict[in_key]["transfer_in_count"] += 1

            out_key = (block_timestamp, block_number, transfer.from_address, token_address)

            address_token_swap_dict[out_key]["transfer_out_amount"] += transfer.value / 10**decimals
            # address_token_swap_dict[in_key]['transfer_out_amount_usd'] += 0
            address_token_swap_dict[out_key]["transfer_out_count"] += 1

        for k, v in address_token_swap_dict.items():
            block_timestamp, block_number, trader_id, token_address = k
            if not trader_id or not token_address:
                continue

            # if trader_id not in self.smart_money_address_list:
            #     continue

            if token_address in self.config:
                continue

            if v["swap_in_amount"]:
                v["transfer_in_amount_usd"] = v["swap_in_amount_usd"] / v["swap_in_amount"] * v["transfer_in_amount"]

            if v["swap_out_amount"]:
                v["transfer_out_amount_usd"] = (
                    v["swap_out_amount_usd"] / v["swap_out_amount"] * v["transfer_out_amount"]
                )

            address_swap_domain = SmartMoneySignalMetrics(
                block_timestamp=block_timestamp,
                block_number=block_number,
                trader_id=trader_id,
                token_address=token_address,
                **v,
            )

            if address_swap_domain.token_address and address_swap_domain.trader_id:
                self._collect_domain(address_swap_domain)
