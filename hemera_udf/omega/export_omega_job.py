#!/usr/bin/env python
# -*- coding: utf-8 -*-

from typing import List, Union

from hemera.indexer.domains.log import Log
from hemera.indexer.domains.transaction import Transaction
from hemera.indexer.jobs.base_job import Collector, FilterTransactionDataJob
from hemera.indexer.specification.specification import TopicSpecification, TransactionFilterByLogs
from hemera_udf.omega.abis import *
from hemera_udf.omega.domains import (
    OmegaAccount,
    OmegaAccountEvent,
    OmegaAccountStrategyActivate,
    OmegaAccountStrategyDeactivate,
    OmegaEvent,
    OmegaStrategyDeposit,
    OmegaStrategyWithdraw,
)


# Also could inherit from FilterTransactionDataJob, like: class ExportOmegaJob(FilterTransactionDataJob):
class ExportOmegaJob(FilterTransactionDataJob):
    # If the data processing logic of the developed job supports re-running with on-chain data reorg,
    # set able_to_reorg to True so that the scheduler can automatically start the job during the reorg process.
    able_to_reorg = False

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.pools = {
            "0x1b69264ca6e951b14db2cf330de2744524a22040": "0xdeaddeaddeaddeaddeaddeaddeaddeaddead1111",  # weth
            "0x68f108f6bdbe14b77f5d042b1b43bb36c60f8580": "0xcda86a272531e8640cd7f1a92c01839911b90bb0",  # meth
            "0x0819ec86bf7278547b6962392f49fa0e88a04b7b": "0xe6829d9a7ee3040e1276fa75293bde931859e8fa",  # cmeth
            "0x72c7d27320e042417506e594697324db5fbf334c": "0xc96de26018a54d51c097160568752c4e3bd6c364",  # fbtc
            "0xd2698b234b23966258578e0539a5d5aab8d49893": "0xe6829d9a7ee3040e1276fa75293bde931859e8fa",  # cmeth manager
            "0x0e27103cd0002ed9694e8865befd6e2167132ba9": "0xc96de26018a54d51c097160568752c4e3bd6c364",  # fbtc manager
            "0x4ebfab8c6dcb6ccaa21bdd2b70e3614064844c47": "0xcda86a272531e8640cd7f1a92c01839911b90bb0",  # meth
            "0xa803861ae852cb34a4fd8f1b756c0ce3b29a2928": "0x5d3a1ff2b6bab83b63cd9ad0787074081a52ef34",  # usde
            "0x382c41175ebc9c906fb52148affd7afb5158eccf": "0x5d3a1ff2b6bab83b63cd9ad0787074081a52ef34",  # usde manager
        }

    # If the data you need to process is only part of the contract log, event, or a specific type of transaction,
    # you can inherit the custom class from FilterTransactionDataJob instead of ExtensionJob, and implement the self.get_filter method.
    # This will speed up the efficiency of the previous task and only index some relevant data.
    def get_filter(self):

        filter = [
            TransactionFilterByLogs(
                [
                    TopicSpecification(
                        addresses=list(self.pools.keys()),
                        topics=[
                            OMEGA_DEPOSIT_EVENT.get_signature(),
                            OMEGA_BORROW_EVENT.get_signature(),
                            OMEGA_WITHDRAW_EVENT.get_signature(),
                            OMEGA_REPAY_EVENT.get_signature(),
                            OMEGA_ACCOUNT_CREATE_EVENT.get_signature(),
                            OMEGA_ACCOUNT_COLLATERAL_DEPOSIT_EVENT.get_signature(),
                            OMEGA_ACCOUNT_COLLATERAL_WITHDRAW_EVENT.get_signature(),
                            OMEGA_ACCOUNT_COLLATERAL_LIQUIDATION_EVENT.get_signature(),
                            OMEGA_ACCOUNT_BORROW_EVENT.get_signature(),
                            OMEGA_ACCOUNT_STRATEGY_ACTIVATE_EVENT.get_signature(),
                            OMEGA_ACCOUNT_STRATEGY_DEACTIVATE_EVENT.get_signature(),
                        ],
                    ),
                    TopicSpecification(
                        topics=[
                            OMEGA_ACCOUNT_STRATEGY_WITHDRAW_EVENT.get_signature(),
                            OMEGA_ACCOUNT_STRATEGY_DEPOSIT_EVENT.get_signature(),
                        ]
                    ),
                ]
            ),
        ]

        return filter

    def _udf(
        self,
        logs: List[Log],
        output: Collector[
            Union[
                OmegaAccount,
                OmegaEvent,
                OmegaAccountEvent,
                OmegaAccountStrategyActivate,
                OmegaAccountStrategyDeactivate,
                OmegaStrategyDeposit,
                OmegaStrategyWithdraw,
            ]
        ],
    ):

        borrow_event_map = {}
        repay_event_map = {}

        for log in logs:
            if log.topic0 == OMEGA_ACCOUNT_CREATE_EVENT.get_signature():
                decoded_data = OMEGA_ACCOUNT_CREATE_EVENT.decode_log(log)
                owner = decoded_data["owner"]
                account = decoded_data["account"]
                new_account = OmegaAccount(
                    owner=owner,
                    account=account,
                    block_number=log.block_number,
                    block_timestamp=log.block_timestamp,
                )
                output.collect_item(OmegaAccount.type(), new_account)

            elif log.topic0 == OMEGA_DEPOSIT_EVENT.get_signature():
                decoded_data = OMEGA_DEPOSIT_EVENT.decode_log(log)
                address = decoded_data["lender"]
                amount = decoded_data["amount"]
                omega_event = OmegaEvent(
                    address=address,
                    event_type=OMEGA_DEPOSIT_EVENT.get_name(),
                    asset_address=self.pools[log.address],
                    amount=amount,
                    receiver_address=address,
                    block_number=log.block_number,
                    block_timestamp=log.block_timestamp,
                    transaction_hash=log.transaction_hash,
                    log_index=log.log_index,
                )
                output.collect_item(OmegaEvent.type(), omega_event)

            elif log.topic0 == OMEGA_BORROW_EVENT.get_signature():
                decoded_data = OMEGA_BORROW_EVENT.decode_log(log)
                address = decoded_data["borrower"]
                amount = decoded_data["amount"]
                omega_event = OmegaEvent(
                    address=address,
                    event_type=OMEGA_BORROW_EVENT.get_name(),
                    asset_address=self.pools[log.address],
                    amount=amount,
                    receiver_address=address,
                    block_number=log.block_number,
                    block_timestamp=log.block_timestamp,
                    transaction_hash=log.transaction_hash,
                    log_index=log.log_index,
                )
                output.collect_item(OmegaEvent.type(), omega_event)
                borrow_event_map[log.transaction_hash] = omega_event

            elif log.topic0 == OMEGA_WITHDRAW_EVENT.get_signature():
                decoded_data = OMEGA_WITHDRAW_EVENT.decode_log(log)
                address = decoded_data["lender"]
                recipient = decoded_data["recipient"]
                amount = decoded_data["amount"]
                omega_event = OmegaEvent(
                    address=address,
                    event_type=OMEGA_WITHDRAW_EVENT.get_name(),
                    asset_address=self.pools[log.address],
                    amount=amount,
                    receiver_address=recipient,
                    block_number=log.block_number,
                    block_timestamp=log.block_timestamp,
                    transaction_hash=log.transaction_hash,
                    log_index=log.log_index,
                )
                output.collect_item(OmegaEvent.type(), omega_event)

            elif log.topic0 == OMEGA_REPAY_EVENT.get_signature():
                decoded_data = OMEGA_REPAY_EVENT.decode_log(log)
                address = decoded_data["borrower"]
                amount = decoded_data["amount"]
                omega_event = OmegaEvent(
                    address=address,
                    event_type=OMEGA_REPAY_EVENT.get_name(),
                    asset_address=self.pools[log.address],
                    amount=amount,
                    receiver_address=address,
                    block_number=log.block_number,
                    block_timestamp=log.block_timestamp,
                    transaction_hash=log.transaction_hash,
                    log_index=log.log_index,
                )
                output.collect_item(OmegaEvent.type(), omega_event)
                repay_event_map[log.transaction_hash] = omega_event

            elif log.topic0 == OMEGA_ACCOUNT_COLLATERAL_DEPOSIT_EVENT.get_signature():
                decoded_data = OMEGA_ACCOUNT_COLLATERAL_DEPOSIT_EVENT.decode_log(log)
                owner = decoded_data["owner"]
                account = decoded_data["account"]
                sender = decoded_data["sender"]
                amount = decoded_data["amount"]
                omega_event = OmegaAccountEvent(
                    owner=owner,
                    account=account,
                    event_type=OMEGA_ACCOUNT_COLLATERAL_DEPOSIT_EVENT.get_name(),
                    asset_address=self.pools[log.address],
                    amount=amount,
                    receiver_address=sender,
                    block_number=log.block_number,
                    block_timestamp=log.block_timestamp,
                    transaction_hash=log.transaction_hash,
                    log_index=log.log_index,
                )
                output.collect_item(OmegaAccountEvent.type(), omega_event)

            elif log.topic0 == OMEGA_ACCOUNT_COLLATERAL_WITHDRAW_EVENT.get_signature():
                decoded_data = OMEGA_ACCOUNT_COLLATERAL_WITHDRAW_EVENT.decode_log(log)
                owner = decoded_data["owner"]
                account = decoded_data["account"]
                receiver = decoded_data["receiver"]
                amount = decoded_data["amount"]
                omega_event = OmegaAccountEvent(
                    owner=owner,
                    account=account,
                    event_type=OMEGA_ACCOUNT_COLLATERAL_WITHDRAW_EVENT.get_name(),
                    asset_address=self.pools[log.address],
                    amount=amount,
                    receiver_address=receiver,
                    block_number=log.block_number,
                    block_timestamp=log.block_timestamp,
                    transaction_hash=log.transaction_hash,
                    log_index=log.log_index,
                )
                output.collect_item(OmegaAccountEvent.type(), omega_event)

            elif log.topic0 == OMEGA_ACCOUNT_BORROW_EVENT.get_signature():
                decoded_data = OMEGA_ACCOUNT_BORROW_EVENT.decode_log(log)
                owner = decoded_data["owner"]
                account = decoded_data["account"]
                asset = decoded_data["asset"]
                amount = decoded_data["amount"]
                omega_event = OmegaAccountEvent(
                    owner=owner,
                    account=account,
                    event_type=OMEGA_ACCOUNT_BORROW_EVENT.get_name(),
                    asset_address=asset,
                    amount=amount,
                    receiver_address=account,
                    block_number=log.block_number,
                    block_timestamp=log.block_timestamp,
                    transaction_hash=log.transaction_hash,
                    log_index=log.log_index,
                )
                output.collect_item(OmegaAccountEvent.type(), omega_event)
            elif log.topic0 == OMEGA_ACCOUNT_STRATEGY_DEPOSIT_EVENT.get_signature():
                decoded_data = OMEGA_ACCOUNT_STRATEGY_DEPOSIT_EVENT.decode_log(log)
                strategy = decoded_data["strategy"]
                amount = decoded_data["amount"]
                borrow_amount = decoded_data["borrowedAmount"]
                shares = decoded_data["shares"]
                omega_event = OmegaStrategyDeposit(
                    strategy=strategy,
                    amount=amount,
                    borrow_amount=borrow_amount,
                    shares=shares,
                    block_number=log.block_number,
                    block_timestamp=log.block_timestamp,
                    transaction_hash=log.transaction_hash,
                    log_index=log.log_index,
                )
                output.collect_item(OmegaStrategyDeposit.type(), omega_event)

            elif log.topic0 == OMEGA_ACCOUNT_STRATEGY_WITHDRAW_EVENT.get_signature():
                decoded_data = OMEGA_ACCOUNT_STRATEGY_WITHDRAW_EVENT.decode_log(log)
                strategy = decoded_data["strategy"]
                amount = decoded_data["amount"]
                shares = decoded_data["shares"]
                repay_amount = sum(decoded_data["repaidAmounts"])
                omega_event = OmegaStrategyWithdraw(
                    strategy=strategy,
                    amount=amount,
                    repay_amount=repay_amount,
                    shares=shares,
                    block_number=log.block_number,
                    block_timestamp=log.block_timestamp,
                    transaction_hash=log.transaction_hash,
                    log_index=log.log_index,
                )
                output.collect_item(OmegaStrategyWithdraw.type(), omega_event)

        account_map = {}
        for log in logs:
            if log.topic0 == OMEGA_ACCOUNT_STRATEGY_ACTIVATE_EVENT.get_signature():
                decoded_data = OMEGA_ACCOUNT_STRATEGY_ACTIVATE_EVENT.decode_log(log)
                owner = decoded_data["owner"]
                account = decoded_data["account"]
                strategy = decoded_data["strategy"]

                borrow_event = borrow_event_map.get(log.transaction_hash)

                activate = OmegaAccountStrategyActivate(
                    owner=owner,
                    account=account,
                    strategy=strategy,
                    is_active=True,
                    borrow_amount=borrow_event.amount if borrow_event else 0,
                    block_number=log.block_number,
                    block_timestamp=log.block_timestamp,
                )
                key = f"{owner}{account}{strategy}"
                if key in account_map:
                    account_map[key] = activate if activate.block_number > account_map[key].block_number else account_map[key]
                else:
                    account_map[key] = activate

            elif log.topic0 == OMEGA_ACCOUNT_STRATEGY_DEACTIVATE_EVENT.get_signature():
                decoded_data = OMEGA_ACCOUNT_STRATEGY_DEACTIVATE_EVENT.decode_log(log)
                owner = decoded_data["owner"]
                account = decoded_data["account"]
                strategy = decoded_data["strategy"]

                repay_event = repay_event_map.get(log.transaction_hash)

                deactivate = OmegaAccountStrategyDeactivate(
                    owner=owner,
                    account=account,
                    strategy=strategy,
                    is_active=False,
                    repay_amount=repay_event.amount if repay_event else 0,
                    block_number=log.block_number,
                    block_timestamp=log.block_timestamp,
                )
                
                key = f"{owner}{account}{strategy}"
                if key in account_map:
                    account_map[key] = deactivate if deactivate.block_number > account_map[key].block_number else account_map[key]
                else:
                    account_map[key] = deactivate

        # Distinct OmegaAccountStrategyDeactivate/OmegaAccountStrategyActivate
        for record in account_map.values():
            output.collect_domain(record)