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

CREATE INDEX idx_af_uniswap_v4_pools_pool_address ON af_uniswap_v4_pools(pool_address);
CREATE INDEX idx_af_uniswap_v4_pools_factory_address ON af_uniswap_v4_pools(factory_address);
CREATE INDEX idx_af_uniswap_v4_pools_token0_address ON af_uniswap_v4_pools(token0_address);
CREATE INDEX idx_af_uniswap_v4_pools_token1_address ON af_uniswap_v4_pools(token1_address);

CREATE INDEX idx_af_uniswap_v4_hooks_hook_address ON af_uniswap_v4_hooks(hook_address);
CREATE INDEX idx_af_uniswap_v4_hooks_factory_address ON af_uniswap_v4_hooks(factory_address);