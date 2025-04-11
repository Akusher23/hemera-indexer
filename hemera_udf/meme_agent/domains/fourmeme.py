from dataclasses import dataclass

from hemera.indexer.domains import Domain


@dataclass
class FourMemeTokenCreateD(Domain):
    """Token creation event from FourMeme"""
    creator: str
    token: str
    request_id: int
    name: str
    symbol: str
    total_supply: int
    launch_time: int
    launch_fee: int
    block_number: int
    block_timestamp: int


@dataclass
class FourMemeTokenTradeD(Domain):
    """Token trading event (buy/sell) from FourMeme"""
    token: str
    account: str
    price: int
    amount: int
    cost: int
    fee: int
    offers: int
    funds: int
    block_number: int
    block_timestamp: int
    # Type of trade: 'buy' or 'sell'
    trade_type: str 