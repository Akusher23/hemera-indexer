from dataclasses import dataclass

from hemera.indexer.domains import Domain


@dataclass
class SmartMoneySignalTrade(Domain):
    block_timestamp: int
    block_number: int
    trader_id: str
    token_address: str
    pool_address: str
    transaction_hash: str
    log_index: int
    token_price: float

    swap_in_amount: float
    swap_in_amount_usd: float
    # swap_in_count: int
    swap_out_amount: float
    swap_out_amount_usd: float
    # swap_out_count: int
