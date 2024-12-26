import logging

from hemera.indexer.domains.block import Block
from hemera.indexer.jobs.base_job import ExtensionJob
from hemera.indexer.utils.multicall_hemera.multi_call_helper import MultiCallHelper
from hemera_udf.token_price.domains import BlockTokenPrice

logger = logging.getLogger(__name__)


class ExportBlockTokenPriceJob(ExtensionJob):
    dependency_types = [Block]

    output_types = [BlockTokenPrice]
    able_to_reorg = True

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # config = kwargs["config"].get("total_supply_job", {})

    def _process(self, **kwargs):
        blocks = self._data_buff[Block.type()]

        for block in blocks:
            block_token_price = BlockTokenPrice(token_symbol='', token_price=1, block_number=block.number)
            self._collect_domain(block_token_price)

        pass

    # def get_token_price(self, start_block, end_block):
    #     sql =
