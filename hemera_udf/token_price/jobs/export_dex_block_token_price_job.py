import logging

import pandas as pd

from hemera.indexer.jobs.base_job import ExtensionJob
from hemera.indexer.utils.collection_utils import distinct_collections_by_group
from hemera_udf.token_price.domains import DexBlockTokenPrice, DexBlockTokenPriceCurrent
from hemera_udf.uniswap_v2 import UniswapV2SwapEvent
from hemera_udf.uniswap_v3 import UniswapV3SwapEvent

logger = logging.getLogger(__name__)


class ExportDexBlockTokenPriceJob(ExtensionJob):
    dependency_types = [UniswapV2SwapEvent, UniswapV3SwapEvent]

    output_types = [DexBlockTokenPrice, DexBlockTokenPriceCurrent]
    able_to_reorg = True

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @staticmethod
    def dataclass_to_df(dataclass):
        dataclass_list = [dc.__dict__ for dc in dataclass]
        df = pd.DataFrame(dataclass_list)
        return df

    @staticmethod
    def process_swap_df(df):
        token0_df = df[
            ["block_number", "block_timestamp", "token0_address", "token0_price", "amount0", "amount_usd"]].rename(
            columns={
                "token0_address": "token_address",
                "token0_price": "token_price",
                "amount0": "amount"
            }
        )
        token1_df = df[
            ["block_number", "block_timestamp", "token1_address", "token1_price", "amount1", "amount_usd"]].rename(
            columns={
                "token1_address": "token_address",
                "token1_price": "token_price",
                "amount1": "amount"
            }
        )
        return pd.concat([token0_df, token1_df], ignore_index=True)

    @staticmethod
    def extract_current_status(records, current_status_domain, keys):
        results = []
        last_records = distinct_collections_by_group(collections=records, group_by=keys, max_key="block_number")
        for last_record in last_records:
            record = current_status_domain(**vars(last_record))
            results.append(record)
        return results

    def _process(self, **kwargs):
        unswapv2_df = self.dataclass_to_df(self._data_buff[UniswapV2SwapEvent.type()])
        unswapv3_df = self.dataclass_to_df(self._data_buff[UniswapV3SwapEvent.type()])

        processed_v2 = self.process_swap_df(unswapv2_df)
        processed_v3 = self.process_swap_df(unswapv3_df)

        combined_df = pd.concat([processed_v2, processed_v3], ignore_index=True)

        results = combined_df.groupby(["token_address", "block_number", "block_timestamp"]).agg(
            token_price=("token_price", "median"),
            amount=("amount", lambda x: x.abs().sum()),
            amount_usd=("amount_usd", "sum")
        ).reset_index()

        records = results.to_dict("records")

        dex_block_token_price_list = []
        for record in records:
            token_dict = self.tokens.get(record.get("token_address"), {})
            token_symbol = token_dict.get("symbol")
            decimals = token_dict.get("decimals")

            amount = record.get('amount') / 10 ** decimals
            record.pop('amount')

            dex_block_token_price = DexBlockTokenPrice(**record, amount=amount, token_symbol=token_symbol,
                                                       decimals=decimals)

            dex_block_token_price_list.append(dex_block_token_price)
        self._collect_domains(dex_block_token_price_list)

        current_records = self.extract_current_status(dex_block_token_price_list, DexBlockTokenPriceCurrent, ["token_address"])
        self._collect_domains(current_records)
        pass
