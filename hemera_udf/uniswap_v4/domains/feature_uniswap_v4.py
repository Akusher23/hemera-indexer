from dataclasses import dataclass

from hemera.indexer.domains import Domain


@dataclass
class UniswapV4Pool(Domain):
    position_token_address: str
    factory_address: str
    pool_address: str
    token0_address: str
    token1_address: str
    fee: int
    tick_spacing: int
    block_number: int
    block_timestamp: int
    hook_address: str 


@dataclass
class UniswapV4Token(Domain):
    position_token_address: str
    token_id: int
    pool_address: str
    tick_lower: int
    tick_upper: int
    fee: int
    block_number: int
    block_timestamp: int


@dataclass
class UniswapV4PoolPrice(Domain):
    factory_address: str
    pool_address: str
    sqrt_price_x96: int
    tick: int
    token0_price: float
    token1_price: float
    block_number: int
    block_timestamp: int


@dataclass
class UniswapV4TokenDetail(Domain):
    position_token_address: str
    token_id: int
    pool_address: str
    wallet_address: str
    liquidity: int
    block_number: int
    block_timestamp: int


@dataclass
class UniswapV4PoolCurrentPrice(Domain):
    factory_address: str
    pool_address: str
    sqrt_price_x96: int
    tick: int
    token0_price: float
    token1_price: float
    block_number: int
    block_timestamp: int


@dataclass
class UniswapV4SwapEvent(Domain):
    pool_address: str
    position_token_address: str
    transaction_from_address: str
    sender: str
    recipient: str
    amount0: int
    amount1: int
    token0_price: float
    token1_price: float
    amount_usd: float
    liquidity: int
    tick: int
    sqrt_price_x96: int
    token0_address: str
    token1_address: str
    transaction_hash: str
    log_index: int
    block_number: int
    block_timestamp: int
    hook_data: str = None  # JSON string of hook-related data
    is_eth_swap: bool = False  # Whether this swap involves native ETH


@dataclass
class UniswapV4ETHSwapEvent(Domain):
    """Specific for tracking swap events involving native ETH"""
    pool_address: str
    eth_amount: int  # Positive value means ETH in, negative value means ETH out
    token_address: str  # The token address paired with ETH
    token_amount: int
    transaction_hash: str
    log_index: int
    wallet_address: str  # The wallet address performing the swap
    block_number: int
    block_timestamp: int
    hook_address: str  # WETH hook address
    is_eth_to_token: bool  # Swap direction: ETH->token or token->ETH


@dataclass
class UniswapV4TokenCurrentStatus(Domain):
    position_token_address: str
    token_id: int
    pool_address: str
    wallet_address: str
    liquidity: int
    block_number: int
    block_timestamp: int


@dataclass
class UniswapV4TokenUpdateLiquidity(Domain):
    position_token_address: str
    token_id: int
    owner: str
    liquidity: int
    amount0: int
    amount1: int
    action_type: str
    transaction_hash: str
    pool_address: str
    token0_address: str
    token1_address: str
    log_index: int
    block_number: int
    block_timestamp: int


@dataclass
class UniswapV4TokenCollectFee(Domain):
    position_token_address: str
    recipient: str
    owner: str
    token_id: int
    amount0: int
    amount1: int
    pool_address: str
    token0_address: str
    token1_address: str
    transaction_hash: str
    log_index: int
    block_number: int
    block_timestamp: int


@dataclass
class UniswapV4PoolFromSwapEvent(UniswapV4Pool):
    pass


@dataclass
class UniswapV4PoolFromToken(UniswapV4Pool):
    pass


@dataclass
class UniswapV4Hook(Domain):
    hook_address: str
    factory_address: str
    pool_address: str
    hook_type: str  # e.g., "fee", "dynamic_fee", "limit_order", etc.
    hook_data: str  # JSON string of hook-specific data
    block_number: int
    block_timestamp: int


@dataclass
class UniswapV4HookEvent(Domain):
    hook_address: str
    factory_address: str
    pool_address: str
    hook_type: str
    event_type: str  # e.g., "before_swap", "after_swap", etc.
    event_data: str  # JSON string of event-specific data
    transaction_hash: str
    log_index: int
    block_number: int
    block_timestamp: int 