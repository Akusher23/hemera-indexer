import logging
from enum import Enum
from itertools import groupby
from typing import List, Union

from hemera.common.utils.web3_utils import ZERO_ADDRESS
from hemera.indexer.domains.contract_internal_transaction import ContractInternalTransaction
from hemera.indexer.domains.token_id_infos import UpdateERC721TokenIdDetail
from hemera.indexer.domains.token_transfer import ERC20TokenTransfer, ERC721TokenTransfer, ERC1155TokenTransfer
from hemera.indexer.domains.transaction import Transaction
from hemera.indexer.executors.batch_work_executor import BatchWorkExecutor
from hemera.indexer.jobs.base_job import ExtensionJob
from hemera.indexer.jobs.export_token_balances_job import extract_token_parameters
from hemera.indexer.jobs.export_token_id_infos_job import generate_token_id_info
from hemera.indexer.utils.collection_utils import distinct_collections_by_group
from hemera.indexer.utils.token_fetcher import TokenFetcher
from hemera_udf.address_index.domains import *

logger = logging.getLogger(__name__)


class InternalTransactionType(Enum):
    SELF_CALL = 0
    SENDER = 1
    RECEIVER = 2


class AddressTransactionType(Enum):
    SELF_CALL = 0

    SENDER = 1
    RECEIVER = 2

    CREATOR = 3
    BEEN_CREATED = 4


class AddressTokenTransferType(Enum):
    SELF_CALL = 0

    SENDER = 1
    RECEIVER = 2

    DEPOSITOR = 3
    WITHDRAWER = 4


class AddressNftTransferType(Enum):
    SELF_CALL = 0

    SENDER = 1
    RECEIVER = 2

    BURNER = 3
    MINTER = 4


def create_address_internal_transaction(
    internal_transaction: ContractInternalTransaction,
    address: str,
    txn_type: int,
    related_address: str,
    transaction_receipt_status: int,
):
    yield AddressInternalTransaction(
        address=address,
        trace_id=internal_transaction.trace_id,
        block_number=internal_transaction.block_number,
        transaction_index=internal_transaction.transaction_index,
        transaction_hash=internal_transaction.transaction_hash,
        block_timestamp=internal_transaction.block_timestamp,
        block_hash=internal_transaction.block_hash,
        error=internal_transaction.error,
        status=int(internal_transaction.status or 0),
        input_method=(internal_transaction.input or "")[2:10],
        value=internal_transaction.value,
        gas=internal_transaction.gas,
        gas_used=internal_transaction.gas_used,
        trace_type=internal_transaction.trace_type,
        call_type=internal_transaction.call_type,
        txn_type=txn_type,
        related_address=related_address,
        transaction_receipt_status=transaction_receipt_status,
    )


def create_address_contract_operation(
    internal_transaction: ContractInternalTransaction,
    address: str,
    contract_address: str,
    transaction_receipt_status: int,
):
    yield AddressContractOperation(
        address=address,
        trace_from_address=internal_transaction.from_address,
        contract_address=contract_address,
        trace_id=internal_transaction.trace_id,
        block_number=internal_transaction.block_number,
        transaction_index=internal_transaction.transaction_index,
        transaction_hash=internal_transaction.transaction_hash,
        block_timestamp=internal_transaction.block_timestamp,
        block_hash=internal_transaction.block_hash,
        error=internal_transaction.error,
        status=int(internal_transaction.status or 0),
        creation_code=internal_transaction.input,
        deployed_code=internal_transaction.output,
        gas=internal_transaction.gas,
        gas_used=internal_transaction.gas_used,
        trace_type=internal_transaction.trace_type,
        call_type=internal_transaction.call_type,
        transaction_receipt_status=transaction_receipt_status,
    )


def internal_transactions_to_address_internal_transactions(
    internal_transactions: List[ContractInternalTransaction], transaction_dict: dict[str, Transaction]
) -> list[Union[AddressInternalTransaction, AddressContractOperation]]:
    for internal_transaction in internal_transactions:
        if internal_transaction.from_address != internal_transaction.to_address:
            yield from create_address_internal_transaction(
                internal_transaction,
                internal_transaction.from_address,
                InternalTransactionType.SENDER.value,
                internal_transaction.to_address,
                transaction_dict[internal_transaction.transaction_hash].receipt.status,
            )
            if internal_transaction.is_contract_creation():
                yield from create_address_contract_operation(
                    internal_transaction,
                    transaction_dict[internal_transaction.transaction_hash].from_address,
                    internal_transaction.to_address,
                    transaction_dict[internal_transaction.transaction_hash].receipt.status,
                )
            if internal_transaction.to_address is not None:
                yield from create_address_internal_transaction(
                    internal_transaction,
                    internal_transaction.to_address,
                    InternalTransactionType.RECEIVER.value,
                    internal_transaction.from_address,
                    transaction_dict[internal_transaction.transaction_hash].receipt.status,
                )
        else:
            yield from create_address_internal_transaction(
                internal_transaction,
                internal_transaction.from_address,
                InternalTransactionType.SELF_CALL.value,
                internal_transaction.to_address,
                transaction_dict[internal_transaction.transaction_hash].receipt.status,
            )


def create_address_transaction(transaction, address, txn_type, related_address, transaction_fee):
    return AddressTransaction(
        address=address,
        block_number=transaction.block_number,
        transaction_index=transaction.transaction_index,
        transaction_hash=transaction.hash,
        block_timestamp=transaction.block_timestamp,
        block_hash=transaction.block_hash,
        txn_type=txn_type,
        related_address=related_address,
        value=transaction.value,
        transaction_fee=transaction_fee,
        receipt_status=transaction.receipt.status,
        method=transaction.input[2:10],
    )


def transactions_to_address_transactions(transactions: List[Transaction]):
    for transaction in transactions:
        transaction_fee = transaction.receipt.gas_used * transaction.gas_price + (transaction.receipt.l1_fee or 0)
        assert transaction.from_address is not None
        assert transaction.to_address is not None or transaction.receipt.contract_address is not None

        if transaction.from_address != transaction.to_address:
            if transaction.to_address is not None:
                yield create_address_transaction(
                    transaction,
                    transaction.to_address,
                    AddressTransactionType.RECEIVER.value,
                    transaction.from_address,
                    transaction_fee,
                )
                yield create_address_transaction(
                    transaction,
                    transaction.from_address,
                    AddressTransactionType.SENDER.value,
                    transaction.to_address,
                    transaction_fee,
                )
            else:
                yield create_address_transaction(
                    transaction,
                    transaction.receipt.contract_address,
                    AddressTransactionType.BEEN_CREATED.value,
                    transaction.from_address,
                    transaction_fee,
                )
                yield create_address_transaction(
                    transaction,
                    transaction.from_address,
                    AddressTransactionType.CREATOR.value,
                    transaction.receipt.contract_address,
                    transaction_fee,
                )
        else:
            yield create_address_transaction(
                transaction,
                transaction.from_address,
                AddressTransactionType.SELF_CALL.value,
                transaction.from_address,
                transaction_fee,
            )


def create_address_token_transfer(token_transfer: ERC20TokenTransfer, address, transfer_type, related_address):
    return AddressTokenTransfer(
        address=address,
        block_number=token_transfer.block_number,
        log_index=token_transfer.log_index,
        transaction_hash=token_transfer.transaction_hash,
        block_timestamp=token_transfer.block_timestamp,
        block_hash=token_transfer.block_hash,
        token_address=token_transfer.token_address,
        related_address=related_address,
        transfer_type=transfer_type,
        value=token_transfer.value,
    )


def erc20_transfers_to_address_token_transfers(transfers: List[ERC20TokenTransfer]):
    for transfer in transfers:
        assert transfer.from_address is not None
        assert transfer.to_address is not None

        if transfer.from_address != transfer.to_address:
            if transfer.from_address == ZERO_ADDRESS:
                yield create_address_token_transfer(
                    transfer, transfer.to_address, AddressTokenTransferType.WITHDRAWER.value, transfer.from_address
                )
            elif transfer.to_address == ZERO_ADDRESS:
                yield create_address_token_transfer(
                    transfer, transfer.from_address, AddressTokenTransferType.DEPOSITOR.value, transfer.to_address
                )
            else:
                yield create_address_token_transfer(
                    transfer, transfer.to_address, AddressTokenTransferType.RECEIVER.value, transfer.from_address
                )
                yield create_address_token_transfer(
                    transfer, transfer.from_address, AddressTokenTransferType.SENDER.value, transfer.to_address
                )
        else:
            yield create_address_token_transfer(
                transfer, transfer.from_address, AddressTokenTransferType.SELF_CALL.value, transfer.from_address
            )


def create_address_nft_transfer(
    nft_transfer: Union[ERC721TokenTransfer, ERC1155TokenTransfer], address, transfer_type, related_address
):
    return AddressNftTransfer(
        address=address,
        block_number=nft_transfer.block_number,
        log_index=nft_transfer.log_index,
        transaction_hash=nft_transfer.transaction_hash,
        block_timestamp=nft_transfer.block_timestamp,
        block_hash=nft_transfer.block_hash,
        token_address=nft_transfer.token_address,
        related_address=related_address,
        transfer_type=transfer_type,
        token_id=nft_transfer.token_id,
        value=nft_transfer.value if isinstance(nft_transfer, ERC1155TokenTransfer) else 1,
    )


def nft_transfers_to_address_nft_transfers(transfers: Union[List[ERC721TokenTransfer], List[ERC1155TokenTransfer]]):
    for transfer in transfers:
        assert transfer.from_address is not None
        assert transfer.to_address is not None

        if transfer.from_address != transfer.to_address:
            if transfer.from_address == ZERO_ADDRESS:
                yield create_address_nft_transfer(
                    transfer, transfer.to_address, AddressNftTransferType.MINTER.value, transfer.from_address
                )
            elif transfer.to_address == ZERO_ADDRESS:
                yield create_address_nft_transfer(
                    transfer, transfer.from_address, AddressNftTransferType.BURNER.value, transfer.to_address
                )
            else:
                yield create_address_nft_transfer(
                    transfer, transfer.to_address, AddressNftTransferType.RECEIVER.value, transfer.from_address
                )
                yield create_address_nft_transfer(
                    transfer, transfer.from_address, AddressNftTransferType.SENDER.value, transfer.to_address
                )
        else:
            yield create_address_nft_transfer(
                transfer, transfer.from_address, AddressNftTransferType.SELF_CALL.value, transfer.from_address
            )


class AddressIndexerJob(ExtensionJob):
    dependency_types = [Transaction, ERC20TokenTransfer, ERC721TokenTransfer, ERC1155TokenTransfer]
    output_types = [
        AddressTransaction,
        AddressTokenTransfer,
        AddressNftTransfer,
    ]

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._batch_work_executor = BatchWorkExecutor(
            kwargs["batch_size"],
            kwargs["max_workers"],
            job_name=self.__class__.__name__,
        )
        self.token_fetcher = TokenFetcher(self._web3, kwargs)
        self._is_multi_call = kwargs["multicall"]

    def _process(self, **kwargs):

        transactions = self._get_domain(Transaction)
        self._collect_domains(list(transactions_to_address_transactions(transactions)))

        token_transfers = self._get_domain(ERC20TokenTransfer)
        a = list(erc20_transfers_to_address_token_transfers(token_transfers))
        print(a)
        self._collect_domains(list(erc20_transfers_to_address_token_transfers(token_transfers)))

        nft_transfers = self._get_domain(ERC721TokenTransfer)
        self._collect_domains(list(nft_transfers_to_address_nft_transfers(nft_transfers)))

        erc1155_transfers = self._get_domain(ERC1155TokenTransfer)
        self._collect_domains(list(nft_transfers_to_address_nft_transfers(erc1155_transfers)))
