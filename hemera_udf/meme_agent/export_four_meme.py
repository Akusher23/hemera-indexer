import logging
from typing import List

from hemera.indexer.domains.log import Log
from hemera.indexer.domains.transaction import Transaction
from hemera.indexer.jobs import FilterTransactionDataJob
from hemera.indexer.specification.specification import TopicSpecification, TransactionFilterByLogs

from hemera_udf.meme_agent.abi.fourmeme_event import token_create_event, token_purchase_event, token_sale_event
from hemera_udf.meme_agent.domains.fourmeme import (
    FourMemeTokenCreateD,
    FourMemeTokenTradeD,
)
from hemera_udf.meme_agent.models.fourmeme import FourMemeTokenCreate, FourMemeTokenTrade

logger = logging.getLogger(__name__)


class ExportFourMemeJob(FilterTransactionDataJob):
    """Job for exporting FourMeme protocol events"""
    dependency_types = [Log]
    output_types = [FourMemeTokenCreateD, FourMemeTokenTradeD]
    able_to_reorg = True

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.events = {
            token_create_event.get_signature(): token_create_event,
            token_purchase_event.get_signature(): token_purchase_event,
            token_sale_event.get_signature(): token_sale_event
        }

    def get_filter(self):
        """Get event filter specification"""
        return TransactionFilterByLogs(
            [
                TopicSpecification(
                    addresses=[self.user_defined_config.get("token_manager2_addresses")],
                    topics=[
                        token_create_event.get_signature(),
                        token_purchase_event.get_signature(),
                        token_sale_event.get_signature()
                    ]
                )
            ]
        )

    def _collect(self, **kwargs):
        pass

    def _process(self, **kwargs):
        """Process log events"""
        logs: List[Log] = self._data_buff.get(Log.type(), [])
        for log in logs:
            if log.topic0 == token_create_event.get_signature():
                self._process_token_create(log)
            elif log.topic0 == token_purchase_event.get_signature():
                self._process_token_purchase(log)
            elif log.topic0 == token_sale_event.get_signature():
                self._process_token_sale(log)

    def _process_token_create(self, log: Log):
        """Process token creation event"""
        log_data = token_create_event.decode_log(log)
        if not log_data:
            return

        self._collect_domain(
            FourMemeTokenCreateD(
                creator=log_data["creator"],
                token=log_data["token"],
                request_id=log_data["requestId"],
                name=log_data["name"],
                symbol=log_data["symbol"],
                total_supply=log_data["totalSupply"],
                launch_time=log_data["launchTime"],
                launch_fee=log_data["launchFee"],
                block_number=log.block_number,
                block_timestamp=log.block_timestamp,
            )
        )

    def _process_token_purchase(self, log: Log):
        """Process token purchase event"""
        log_data = token_purchase_event.decode_log(log)
        if not log_data:
            return

        self._collect_domain(
            FourMemeTokenTradeD(
                token=log_data["token"],
                account=log_data["account"],
                price=log_data["price"],
                amount=log_data["amount"],
                cost=log_data["cost"],
                fee=log_data["fee"],
                offers=log_data["offers"],
                funds=log_data["funds"],
                block_number=log.block_number,
                block_timestamp=log.block_timestamp,
                trade_type="buy"
            )
        )

    def _process_token_sale(self, log: Log):
        """Process token sale event"""
        log_data = token_sale_event.decode_log(log)
        if not log_data:
            return

        self._collect_domain(
            FourMemeTokenTradeD(
                token=log_data["token"],
                account=log_data["account"],
                price=log_data["price"],
                amount=log_data["amount"],
                cost=log_data["cost"],
                fee=log_data["fee"],
                offers=log_data["offers"],
                funds=log_data["funds"],
                block_number=log.block_number,
                block_timestamp=log.block_timestamp,
                trade_type="sell"
            )
        )
