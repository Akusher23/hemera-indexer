#!/usr/bin/env python
# -*- coding: utf-8 -*-

from typing import List, Union

from hemera.indexer.domains.transaction import Transaction
from hemera.indexer.domains.log import Log
from hemera.indexer.jobs.base_job import ExtensionJob, Collector
from hemera.indexer.specification.specification import TopicSpecification, TransactionFilterByLogs
from hemera_udf.omega.domains import OmegaAccount, OmegaEvent, OmegaAccountEvent
from hemera_udf.omega.abis import *

# Also could inherit from FilterTransactionDataJob, like: class ExportOmegaJob(FilterTransactionDataJob):
class ExportOmegaJob(ExtensionJob):
    # If the data processing logic of the developed job supports re-running with on-chain data reorg,
    # set able_to_reorg to True so that the scheduler can automatically start the job during the reorg process.
    able_to_reorg = False

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.pool_list = [
            "0x1b69264ca6e951b14db2cf330de2744524a22040", # weth
            "0x68f108f6bdbe14b77f5d042b1b43bb36c60f8580", # meth
            "0x0819ec86bf7278547b6962392f49fa0e88a04b7b", # cmeth
            "0x72c7d27320e042417506e594697324db5fbf334c", # fbtc

            "0xd2698b234b23966258578e0539a5d5aab8d49893", # cmeth
            "0x0e27103cd0002ed9694e8865befd6e2167132ba9", # fbtc
        ]

    # If the data you need to process is only part of the contract log, event, or a specific type of transaction,
    # you can inherit the custom class from FilterTransactionDataJob instead of ExtensionJob, and implement the self.get_filter method.
    # This will speed up the efficiency of the previous task and only index some relevant data.
    def get_filter(self):

        filter = [
            TransactionFilterByLogs([TopicSpecification(addresses=self.pool_list, topics=[
                OMEGA_DEPOSIT_EVENT,
                OMEGA_BORROW_EVENT,
                OMEGA_WITHDRAW_EVENT,
                OMEGA_REPAY_EVENT,
                OMEGA_ACCOUNT_CREATE_EVENT,
                OMEGA_ACCOUNT_COLLATERAL_DEPOSIT_EVENT,
                OMEGA_ACCOUNT_COLLATERAL_WITHDRAW_EVENT,
                OMEGA_ACCOUNT_COLLATERAL_LIQUIDATION_EVENT,
            ])]),
        ]

        return filter

    def _udf(self, logs: List[Log], output: Collector[Union[OmegaAccount, OmegaEvent, OmegaAccountEvent]]):
        
        for log in logs:
            if log.topic0 == OMEGA_ACCOUNT_CREATE_EVENT.get_signature():
                decoded_data = OMEGA_ACCOUNT_CREATE_EVENT.decode_log(log)
                owner = decoded_data["owner"]
                account = decoded_data["account"]
                new_account = OmegaAccount(
                    onwer=owner,
                    account=account,
                    block_number=log.block_number,
                    block_timestampr=log.block_timestamp,
                )
                output.collect_item(OmegaAccount.type(), new_account)

            elif log.topic0 == OMEGA_DEPOSIT_EVENT.get_signature():
                decoded_data = OMEGA_DEPOSIT_EVENT.decode_log(log)
                address = decoded_data["lender"]
                amount = decoded_data["amount"]
                omega_event = OmegaEvent(
                    address=address,
                    event_type=OMEGA_DEPOSIT_EVENT.get_name(),
                    asset_address="",
                    amount=amount,
                    receiver_address=address,
                    block_number=log.block_number,
                    block_timestampr=log.block_timestamp,
                )
                output.collect_item(OmegaEvent.type(), omega_event)

            elif log.topic0 == OMEGA_BORROW_EVENT:
                decoded_data = OMEGA_BORROW_EVENT.decode_log(log)
                address = decoded_data["borrower"]
                amount = decoded_data["amount"]
                omega_event = OmegaEvent(
                    address=address,
                    event_type=OMEGA_BORROW_EVENT.get_name(),
                    asset_address="",
                    amount=amount,
                    receiver_address=address,
                    block_number=log.block_number,
                    block_timestampr=log.block_timestamp,
                )
                output.collect_item(OmegaEvent.type(), omega_event)

            elif log.topic0 == OMEGA_WITHDRAW_EVENT:
                decoded_data = OMEGA_WITHDRAW_EVENT.decode_log(log)
                address = decoded_data["lender"]
                recipient = decoded_data["recipient"]
                amount = decoded_data["amount"]
                omega_event = OmegaEvent(
                    address=address,
                    event_type=OMEGA_BORROW_EVENT.get_name(),
                    asset_address="",
                    amount=amount,
                    receiver_address=recipient,
                    block_number=log.block_number,
                    block_timestampr=log.block_timestamp,
                )
                output.collect_item(OmegaEvent.type(), omega_event)

            elif log.topic0 == OMEGA_REPAY_EVENT:
                decoded_data = OMEGA_REPAY_EVENT.decode_log(log)
                address = decoded_data["borrower"]
                amount = decoded_data["amount"]
                omega_event = OmegaEvent(
                    address=address,
                    event_type=OMEGA_BORROW_EVENT.get_name(),
                    asset_address="",
                    amount=amount,
                    receiver_address=address,
                    block_number=log.block_number,
                    block_timestampr=log.block_timestamp,
                )
                output.collect_item(OmegaEvent.type(), omega_event)

            elif log.topic0 == OMEGA_ACCOUNT_COLLATERAL_DEPOSIT_EVENT:
                decoded_data = OMEGA_ACCOUNT_COLLATERAL_DEPOSIT_EVENT.decode_log(log)
                owner = decoded_data["owner"]
                account = decoded_data["account"]
                sender = decoded_data["sender"]
                amount = decoded_data["amount"]
                omega_event = OmegaAccountEvent(
                    owner=owner,
                    account=account,
                    event_type=OMEGA_ACCOUNT_COLLATERAL_DEPOSIT_EVENT.get_name(),
                    asset_address="",
                    amount=amount,
                    receiver_address=sender,
                    block_number=log.block_number,
                    block_timestampr=log.block_timestamp,
                )
                output.collect_item(OmegaAccountEvent.type(), omega_event)

            elif log.topic0 == OMEGA_ACCOUNT_COLLATERAL_WITHDRAW_EVENT:
                decoded_data = OMEGA_ACCOUNT_COLLATERAL_WITHDRAW_EVENT.decode_log(log)
                owner = decoded_data["owner"]
                account = decoded_data["account"]
                receiver = decoded_data["receiver"]
                amount = decoded_data["amount"]
                omega_event = OmegaAccountEvent(
                    owner=owner,
                    account=account,
                    event_type=OMEGA_ACCOUNT_COLLATERAL_WITHDRAW_EVENT.get_name(),
                    asset_address="",
                    amount=amount,
                    receiver_address=receiver,
                    block_number=log.block_number,
                    block_timestampr=log.block_timestamp,
                )
                output.collect_item(OmegaAccountEvent.type(), omega_event)
