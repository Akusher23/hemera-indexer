from dataclasses import dataclass

from hemera.indexer.domains import Domain


@dataclass
class SmartMoneySignalMetrics(Domain):
    block_timestamp: int
    block_number: int
    trader_id: str
    token_address: str

    swap_in_amount: float
    swap_in_amount_usd: float
    swap_in_count: int
    swap_out_amount: float
    swap_out_amount_usd: float
    swap_out_count: int
    # transfer_in_amount: float
    # transfer_in_amount_usd: float
    # transfer_in_count: int
    # transfer_out_amount: float
    # transfer_out_amount_usd: float
    # transfer_out_count: int
