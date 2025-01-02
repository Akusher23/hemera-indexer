import logging

import pandas as pd
from sqlalchemy import and_, func, or_

from hemera.common.utils.format_utils import hex_str_to_bytes, bytes_to_hex_str
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
        if df.empty:
            columns = ["block_number", "block_timestamp", "token_address", "token_price", "amount", "amount_usd"]
            return pd.DataFrame(columns=columns)

        token0_df = df[
            ["block_number", "block_timestamp", "token0_address", "token0_price", "amount0", "amount_usd"]
        ].rename(columns={"token0_address": "token_address", "token0_price": "token_price", "amount0": "amount"})
        token1_df = df[
            ["block_number", "block_timestamp", "token1_address", "token1_price", "amount1", "amount_usd"]
        ].rename(columns={"token1_address": "token_address", "token1_price": "token_price", "amount1": "amount"})
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

        results = (
            combined_df.groupby(["token_address", "block_number", "block_timestamp"])
            .agg(
                token_price=("token_price", "median"),
                amount=("amount", lambda x: x.abs().sum()),
                amount_usd=("amount_usd", "sum"),
            )
            .reset_index()
        )

        records = results.to_dict("records")

        dex_block_token_price_list = []
        for record in records:
            token_price = record.get("token_price")
            if pd.isnull(token_price):
                continue

            if token_price > 200000:
                continue

            token_dict = self.tokens.get(record.get("token_address"), {})
            token_symbol = token_dict.get("symbol")
            if token_symbol is None:
                continue

            decimals = token_dict.get("decimals")

            total_supply = token_dict.get("total_supply")
            if token_price * total_supply / 10 ** decimals > 1880666183880:
                continue

            record["amount"] = record.get("amount") / 10 ** decimals

            dex_block_token_price = DexBlockTokenPrice(**record, token_symbol=token_symbol, decimals=decimals)

            dex_block_token_price_list.append(dex_block_token_price)

        previous_prices = self.get_previous_prices()

        dex_block_token_price_list.sort(key=lambda x: x.block_number)

        results = []

        for dex_record in dex_block_token_price_list:
            token_address = dex_record.token_address

            previous_token_price = previous_prices.get(token_address)
            if previous_token_price:
                if dex_record.token_price / previous_token_price < 1000:
                    previous_prices[token_address] = dex_record.token_price
                    results.append(dex_record)
            else:
                previous_prices[token_address] = dex_record.token_price
                results.append(dex_record)

        self._collect_domains(results)

        current_results = self.extract_current_status(
            results, DexBlockTokenPriceCurrent, ["token_address"]
        )
        self._collect_domains(current_results)
        pass

    def _get_current_holdings(self, tokens, block_number):
        session = self._service.get_service_session()

        conditions = [
            and_(
                DexBlockTokenPrice.token_address == hex_str_to_bytes(token),
            ).self_group()  # need to group for one combination
            for token in tokens
        ]

        windowed_block_number = func.row_number().over(
            partition_by=(
                DexBlockTokenPrice.token_address,
            ),
            order_by=DexBlockTokenPrice.block_number.desc(),
        )

        combined_conditions = or_(*conditions)

        subquery = (
            session.query(DexBlockTokenPrice, windowed_block_number.label("row_number"))
            .filter(combined_conditions, DexBlockTokenPrice.block_number < block_number)
            .subquery()
        )

        query = session.query(subquery).filter(subquery.c.row_number == 1)

        results = query.all()

        pre_prices_dict = {}
        for record in results:
            token_address = bytes_to_hex_str(record.token_address)
            pre_prices_dict[token_address] = record.token_price
        session.close()

        return pre_prices_dict
