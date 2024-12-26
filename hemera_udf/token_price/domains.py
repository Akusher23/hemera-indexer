from dataclasses import dataclass

from hemera.indexer.domains import Domain


@dataclass
class BlockTokenPrice(Domain):
    token_symbol: str
    token_price: float
    block_number: int
