from dataclasses import dataclass

from hemera.indexer.domains import Domain


@dataclass
class CurrentTokenIdBalance(Domain):
    address: str
    token_id: int
    token_address: str
    balance: int
    block_number: int
    block_timestamp: int
