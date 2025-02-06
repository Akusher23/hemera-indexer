from dataclasses import dataclass

from hemera.indexer.domains import Domain


@dataclass
class BlockTokenPrice(Domain):
    token_symbol: str
    token_price: float
    block_number: int


@dataclass
class DexBlockTokenPrice(Domain):
    token_address: str
    token_symbol: str
    decimals: int
    amount: float
    amount_usd: float
    token_price: float

    block_number: int
    block_timestamp: int


@dataclass
class DexBlockTokenPriceCurrent(Domain):
    token_address: str
    token_symbol: str
    decimals: int
    amount: float
    amount_usd: float
    token_price: float

    block_number: int
    block_timestamp: int


@dataclass
class UniswapFilteredSwapEvent(Domain):
    pool_address: str
    transaction_from_address: str
    amount0: int
    amount1: int
    token0_price: float
    token1_price: float
    amount_usd: float
    token0_address: str
    token1_address: str
    transaction_hash: str
    log_index: int
    block_number: int
    block_timestamp: int
    stable_balance: float
    stable_token_symbol: str
    token0_market_cap: float
    token1_market_cap: float
