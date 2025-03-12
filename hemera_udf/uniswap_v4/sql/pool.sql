CREATE TABLE af_uniswap_v4_pools (
    position_token_address BYTEA NOT NULL,
    pool_address BYTEA NOT NULL,
    factory_address BYTEA,
    token0_address BYTEA,
    token1_address BYTEA,
    fee NUMERIC(100),
    tick_spacing NUMERIC(100),
    hook_address BYTEA,
    block_number BIGINT,
    block_timestamp TIMESTAMP,
    create_time TIMESTAMP DEFAULT NOW(),
    update_time TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (position_token_address, pool_address)
);

CREATE TABLE af_uniswap_v4_hooks (
    hook_address BYTEA NOT NULL,
    pool_address BYTEA NOT NULL,
    factory_address BYTEA,
    hook_type TEXT,  -- e.g., "fee", "dynamic_fee", "limit_order", etc.
    hook_data TEXT,  -- JSON string of hook-specific data
    block_number BIGINT,
    block_timestamp TIMESTAMP,
    create_time TIMESTAMP DEFAULT NOW(),
    update_time TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (hook_address, pool_address)
);

-- Positions table to track liquidity positions
CREATE TABLE af_uniswap_v4_positions (
    position_id BYTEA NOT NULL PRIMARY KEY,
    pool_address BYTEA NOT NULL,
    owner_address BYTEA NOT NULL,
    lower_tick BIGINT,
    upper_tick BIGINT,
    liquidity NUMERIC(100),
    token0_amount NUMERIC(100),
    token1_amount NUMERIC(100),
    fees_token0 NUMERIC(100),
    fees_token1 NUMERIC(100),
    block_number BIGINT,
    block_timestamp TIMESTAMP,
    create_time TIMESTAMP DEFAULT NOW(),
    update_time TIMESTAMP DEFAULT NOW()
);

-- Swaps table to track swap events
CREATE TABLE af_uniswap_v4_swaps (
    tx_hash BYTEA NOT NULL,
    log_index BIGINT NOT NULL,
    pool_address BYTEA NOT NULL,
    sender_address BYTEA NOT NULL,
    recipient_address BYTEA NOT NULL,
    token0_delta NUMERIC(100),
    token1_delta NUMERIC(100),
    sqrt_price_x96_before NUMERIC(100),
    sqrt_price_x96_after NUMERIC(100),
    liquidity_before NUMERIC(100),
    liquidity_after NUMERIC(100),
    tick_before BIGINT,
    tick_after BIGINT,
    hook_data TEXT,  -- JSON string of hook-specific data for the swap
    block_number BIGINT,
    block_timestamp TIMESTAMP,
    create_time TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (tx_hash, log_index)
);

-- Liquidity changes table to track mint/burn events
CREATE TABLE af_uniswap_v4_liquidity_changes (
    tx_hash BYTEA NOT NULL,
    log_index BIGINT NOT NULL,
    pool_address BYTEA NOT NULL,
    position_id BYTEA,
    sender_address BYTEA NOT NULL,
    recipient_address BYTEA NOT NULL,
    lower_tick BIGINT,
    upper_tick BIGINT,
    delta_liquidity NUMERIC(100),
    token0_delta NUMERIC(100),
    token1_delta NUMERIC(100),
    event_type TEXT NOT NULL, -- 'mint' or 'burn'
    block_number BIGINT,
    block_timestamp TIMESTAMP,
    create_time TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (tx_hash, log_index)
);

-- Fee collection events
CREATE TABLE af_uniswap_v4_fee_collections (
    tx_hash BYTEA NOT NULL,
    log_index BIGINT NOT NULL,
    pool_address BYTEA NOT NULL,
    position_id BYTEA,
    recipient_address BYTEA NOT NULL,
    fees_token0 NUMERIC(100),
    fees_token1 NUMERIC(100),
    block_number BIGINT,
    block_timestamp TIMESTAMP,
    create_time TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (tx_hash, log_index)
);

-- Ticks table to track active ticks
CREATE TABLE af_uniswap_v4_ticks (
    pool_address BYTEA NOT NULL,
    tick_index BIGINT NOT NULL,
    liquidity_gross NUMERIC(100),
    liquidity_net NUMERIC(100),
    fee_growth_outside0_x128 NUMERIC(100),
    fee_growth_outside1_x128 NUMERIC(100),
    initialized BOOLEAN,
    block_number BIGINT,
    block_timestamp TIMESTAMP,
    create_time TIMESTAMP DEFAULT NOW(),
    update_time TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (pool_address, tick_index)
);

-- Table for tracking factory settings
CREATE TABLE af_uniswap_v4_factory (
    factory_address BYTEA NOT NULL PRIMARY KEY,
    owner_address BYTEA,
    pool_deployer_address BYTEA,
    protocol_fee NUMERIC(100),
    block_number BIGINT,
    block_timestamp TIMESTAMP,
    create_time TIMESTAMP DEFAULT NOW(),
    update_time TIMESTAMP DEFAULT NOW()
);

-- Token details table
CREATE TABLE af_uniswap_v4_tokens (
    token_address BYTEA NOT NULL PRIMARY KEY,
    token_name TEXT,
    token_symbol TEXT,
    token_decimals INTEGER,
    total_supply NUMERIC(100),
    block_number BIGINT,
    block_timestamp TIMESTAMP,
    create_time TIMESTAMP DEFAULT NOW(),
    update_time TIMESTAMP DEFAULT NOW()
);

-- Token current status
CREATE TABLE af_uniswap_v4_token_current_status (
    token_address BYTEA NOT NULL PRIMARY KEY,
    price_usd NUMERIC(100),
    volume_24h NUMERIC(100),
    liquidity_usd NUMERIC(100),
    total_value_locked NUMERIC(100),
    market_cap NUMERIC(100),
    last_updated_timestamp TIMESTAMP,
    create_time TIMESTAMP DEFAULT NOW(),
    update_time TIMESTAMP DEFAULT NOW()
);

-- Token detailed information
CREATE TABLE af_uniswap_v4_token_details (
    token_address BYTEA NOT NULL PRIMARY KEY,
    description TEXT,
    website_url TEXT,
    social_links TEXT, -- JSON format for social media links
    contract_verified BOOLEAN,
    official_status TEXT, -- e.g., "verified", "unverified", "scam", etc.
    create_time TIMESTAMP DEFAULT NOW(),
    update_time TIMESTAMP DEFAULT NOW()
);

-- Pool current prices
CREATE TABLE af_uniswap_v4_pool_current_prices (
    pool_address BYTEA NOT NULL PRIMARY KEY,
    token0_address BYTEA NOT NULL,
    token1_address BYTEA NOT NULL,
    token0_price NUMERIC(100), -- Price of token0 in terms of token1
    token1_price NUMERIC(100), -- Price of token1 in terms of token0
    sqrt_price_x96 NUMERIC(100),
    tick BIGINT,
    liquidity NUMERIC(100),
    token0_price_usd NUMERIC(100),
    token1_price_usd NUMERIC(100),
    last_updated_block_number BIGINT,
    last_updated_timestamp TIMESTAMP,
    create_time TIMESTAMP DEFAULT NOW(),
    update_time TIMESTAMP DEFAULT NOW()
);

-- Pool price history
CREATE TABLE af_uniswap_v4_pool_prices (
    pool_address BYTEA NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    token0_address BYTEA NOT NULL,
    token1_address BYTEA NOT NULL,
    token0_price NUMERIC(100), -- Price of token0 in terms of token1
    token1_price NUMERIC(100), -- Price of token1 in terms of token0
    sqrt_price_x96 NUMERIC(100),
    tick BIGINT,
    liquidity NUMERIC(100),
    token0_price_usd NUMERIC(100),
    token1_price_usd NUMERIC(100),
    block_number BIGINT,
    create_time TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (pool_address, timestamp)
);

-- Ethereum swap records (specifically for ETH swaps)
CREATE TABLE af_uniswap_v4_eth_swap_records (
    tx_hash BYTEA NOT NULL,
    log_index BIGINT NOT NULL,
    pool_address BYTEA NOT NULL,
    eth_address BYTEA NOT NULL, -- Address of ETH or WETH
    token_address BYTEA NOT NULL, -- Address of the other token
    eth_amount NUMERIC(100),
    token_amount NUMERIC(100),
    eth_price_usd NUMERIC(100), -- ETH price in USD at the time of swap
    token_price_usd NUMERIC(100), -- Token price in USD at the time of swap
    sender_address BYTEA NOT NULL,
    recipient_address BYTEA NOT NULL,
    block_number BIGINT,
    block_timestamp TIMESTAMP,
    create_time TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (tx_hash, log_index)
);

-- Hook events table
CREATE TABLE af_uniswap_v4_hook_events (
    tx_hash BYTEA NOT NULL,
    log_index BIGINT NOT NULL,
    pool_address BYTEA NOT NULL,
    hook_address BYTEA NOT NULL,
    event_name TEXT NOT NULL, -- e.g., "InitializeHook", "BeforeSwap", "AfterSwap", etc.
    event_data TEXT, -- JSON string of hook event data
    caller_address BYTEA,
    block_number BIGINT,
    block_timestamp TIMESTAMP,
    create_time TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (tx_hash, log_index)
);

CREATE INDEX idx_af_uniswap_v4_pools_pool_address ON af_uniswap_v4_pools(pool_address);
CREATE INDEX idx_af_uniswap_v4_pools_factory_address ON af_uniswap_v4_pools(factory_address);
CREATE INDEX idx_af_uniswap_v4_pools_token0_address ON af_uniswap_v4_pools(token0_address);
CREATE INDEX idx_af_uniswap_v4_pools_token1_address ON af_uniswap_v4_pools(token1_address);

CREATE INDEX idx_af_uniswap_v4_hooks_hook_address ON af_uniswap_v4_hooks(hook_address);
CREATE INDEX idx_af_uniswap_v4_hooks_factory_address ON af_uniswap_v4_hooks(factory_address);

-- New indices for additional tables
CREATE INDEX idx_af_uniswap_v4_positions_pool_address ON af_uniswap_v4_positions(pool_address);
CREATE INDEX idx_af_uniswap_v4_positions_owner_address ON af_uniswap_v4_positions(owner_address);

CREATE INDEX idx_af_uniswap_v4_swaps_pool_address ON af_uniswap_v4_swaps(pool_address);
CREATE INDEX idx_af_uniswap_v4_swaps_block_timestamp ON af_uniswap_v4_swaps(block_timestamp);

CREATE INDEX idx_af_uniswap_v4_liquidity_changes_pool_address ON af_uniswap_v4_liquidity_changes(pool_address);
CREATE INDEX idx_af_uniswap_v4_liquidity_changes_position_id ON af_uniswap_v4_liquidity_changes(position_id);

CREATE INDEX idx_af_uniswap_v4_fee_collections_pool_address ON af_uniswap_v4_fee_collections(pool_address);
CREATE INDEX idx_af_uniswap_v4_fee_collections_position_id ON af_uniswap_v4_fee_collections(position_id);

CREATE INDEX idx_af_uniswap_v4_ticks_pool_address ON af_uniswap_v4_ticks(pool_address);

-- Indices for token tables
CREATE INDEX idx_af_uniswap_v4_tokens_token_symbol ON af_uniswap_v4_tokens(token_symbol);

-- Indices for pool price tables
CREATE INDEX idx_af_uniswap_v4_pool_prices_timestamp ON af_uniswap_v4_pool_prices(timestamp);
CREATE INDEX idx_af_uniswap_v4_pool_prices_token0_address ON af_uniswap_v4_pool_prices(token0_address);
CREATE INDEX idx_af_uniswap_v4_pool_prices_token1_address ON af_uniswap_v4_pool_prices(token1_address);

-- Indices for ETH swap records
CREATE INDEX idx_af_uniswap_v4_eth_swap_records_pool_address ON af_uniswap_v4_eth_swap_records(pool_address);
CREATE INDEX idx_af_uniswap_v4_eth_swap_records_token_address ON af_uniswap_v4_eth_swap_records(token_address);
CREATE INDEX idx_af_uniswap_v4_eth_swap_records_block_timestamp ON af_uniswap_v4_eth_swap_records(block_timestamp);

-- Indices for hook events
CREATE INDEX idx_af_uniswap_v4_hook_events_hook_address ON af_uniswap_v4_hook_events(hook_address);
CREATE INDEX idx_af_uniswap_v4_hook_events_pool_address ON af_uniswap_v4_hook_events(pool_address);
CREATE INDEX idx_af_uniswap_v4_hook_events_event_name ON af_uniswap_v4_hook_events(event_name);