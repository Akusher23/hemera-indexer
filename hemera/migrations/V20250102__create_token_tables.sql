\set start_partition '2025-01-01'
\set partition_interval '1 month'

-- tokens
CREATE TABLE IF NOT EXISTS tokens
(
    address                  BYTEA NOT NULL
        PRIMARY KEY,
    name                     VARCHAR,
    symbol                   VARCHAR,
    total_supply             NUMERIC(100),
    decimals                 NUMERIC(100),
    token_type               VARCHAR,
    holder_count             INTEGER,
    transfer_count           INTEGER,
    icon_url                 VARCHAR,
    urls                     JSONB,
    volume_24h               NUMERIC(38, 2),
    price                    NUMERIC(38, 6),
    previous_price           NUMERIC(38, 6),
    market_cap               NUMERIC(38, 2),
    on_chain_market_cap      NUMERIC(38, 2),
    is_verified              BOOLEAN,
    cmc_id                   INTEGER,
    cmc_slug                 VARCHAR,
    gecko_id                 VARCHAR,
    description              VARCHAR,
    create_time              TIMESTAMP DEFAULT NOW(),
    update_time              TIMESTAMP DEFAULT NOW(),
    block_number             BIGINT,
    no_balance_of            BOOLEAN DEFAULT FALSE,
    fail_balance_of_count    INTEGER DEFAULT 0,
    no_total_supply          BOOLEAN DEFAULT FALSE,
    fail_total_supply_count  INTEGER DEFAULT 0,
    tags                     CHARACTER VARYING[],
    succeed_balance_of_count INTEGER DEFAULT 0
);

CREATE INDEX IF NOT EXISTS tokens_type_index
    ON tokens (token_type);

CREATE INDEX IF NOT EXISTS tokens_type_holders_index
    ON tokens (token_type ASC, holder_count DESC);

CREATE INDEX IF NOT EXISTS tokens_type_on_chain_market_cap_index
    ON tokens (token_type ASC, on_chain_market_cap DESC);

-- address_token_balances
CREATE TABLE IF NOT EXISTS address_token_balances
(
    address         BYTEA NOT NULL,
    token_address   BYTEA NOT NULL,
    balance         NUMERIC(100),
    block_number    BIGINT NOT NULL,
    block_timestamp TIMESTAMP NOT NULL,
    create_time     TIMESTAMP DEFAULT NOW(),
    update_time     TIMESTAMP DEFAULT NOW(),
    reorg           BOOLEAN DEFAULT FALSE,
    PRIMARY KEY (address, token_address, block_number, block_timestamp)
) PARTITION BY RANGE (block_timestamp);

SELECT partman.create_parent(
               P_PARENT_TABLE := 'public.address_token_balances'::text,
               P_CONTROL := 'block_timestamp'::text,
               P_TYPE := 'range'::text,
               P_INTERVAL := :'partition_interval'::text,
               P_PREMAKE := 12::INT,
               P_AUTOMATIC_MAINTENANCE := 'on'::text,
               P_START_PARTITION := :'start_partition'::text,
               P_DEFAULT_TABLE := FALSE
       );

-- address_current_token_balances
CREATE TABLE IF NOT EXISTS address_current_token_balances
(
    address         BYTEA NOT NULL,
    token_address   BYTEA NOT NULL,
    balance         NUMERIC(100),
    block_number    BIGINT,
    block_timestamp TIMESTAMP,
    create_time     TIMESTAMP DEFAULT NOW(),
    update_time     TIMESTAMP DEFAULT NOW(),
    reorg           BOOLEAN,
    CONSTRAINT address_current_token_balances_partition_pkey
        PRIMARY KEY (address, token_address)
)
    PARTITION BY RANGE (token_address);

CREATE TABLE IF NOT EXISTS address_current_token_balances_p0
    PARTITION OF address_current_token_balances
        FOR VALUES FROM ('\x00') TO ('\x10');

CREATE TABLE IF NOT EXISTS address_current_token_balances_p1
    PARTITION OF address_current_token_balances
        FOR VALUES FROM ('\x10') TO ('\x20');

CREATE TABLE IF NOT EXISTS address_current_token_balances_p2
    PARTITION OF address_current_token_balances
        FOR VALUES FROM ('\x20') TO ('\x30');

CREATE TABLE IF NOT EXISTS address_current_token_balances_p3
    PARTITION OF address_current_token_balances
        FOR VALUES FROM ('\x30') TO ('\x40');

CREATE TABLE IF NOT EXISTS address_current_token_balances_p4
    PARTITION OF address_current_token_balances
        FOR VALUES FROM ('\x40') TO ('\x50');

CREATE TABLE IF NOT EXISTS address_current_token_balances_p5
    PARTITION OF address_current_token_balances
        FOR VALUES FROM ('\x50') TO ('\x60');

CREATE TABLE IF NOT EXISTS address_current_token_balances_p6
    PARTITION OF address_current_token_balances
        FOR VALUES FROM ('\x60') TO ('\x70');

CREATE TABLE IF NOT EXISTS address_current_token_balances_p7
    PARTITION OF address_current_token_balances
        FOR VALUES FROM ('\x70') TO ('\x80');

CREATE TABLE IF NOT EXISTS address_current_token_balances_p8
    PARTITION OF address_current_token_balances
        FOR VALUES FROM ('\x80') TO ('\x90');

CREATE TABLE IF NOT EXISTS address_current_token_balances_p9
    PARTITION OF address_current_token_balances
        FOR VALUES FROM ('\x90') TO ('\xa0');

CREATE TABLE IF NOT EXISTS address_current_token_balances_p10
    PARTITION OF address_current_token_balances
        FOR VALUES FROM ('\xa0') TO ('\xb0');

CREATE TABLE IF NOT EXISTS address_current_token_balances_p11
    PARTITION OF address_current_token_balances
        FOR VALUES FROM ('\xb0') TO ('\xc0');

CREATE TABLE IF NOT EXISTS address_current_token_balances_p12
    PARTITION OF address_current_token_balances
        FOR VALUES FROM ('\xc0') TO ('\xd0');

CREATE TABLE IF NOT EXISTS address_current_token_balances_p13
    PARTITION OF address_current_token_balances
        FOR VALUES FROM ('\xd0') TO ('\xe0');

CREATE TABLE IF NOT EXISTS address_current_token_balances_p14
    PARTITION OF address_current_token_balances
        FOR VALUES FROM ('\xe0') TO ('\xf0');

CREATE TABLE IF NOT EXISTS address_current_token_balances_p15
    PARTITION OF address_current_token_balances
        FOR VALUES FROM ('\xf0') TO ('\xffffffffffffffffffffffffffffffffffffffffff');

-- address_current_token_id_balances
CREATE TABLE IF NOT EXISTS address_current_token_id_balances
(
	address         BYTEA NOT NULL,
	token_address   BYTEA NOT NULL,
	token_id        NUMERIC(78) NOT NULL,
	balance         NUMERIC(100),
	block_number    BIGINT NOT NULL,
	block_timestamp TIMESTAMP NOT NULL,
	create_time     TIMESTAMP DEFAULT NOW(),
	update_time     TIMESTAMP DEFAULT NOW(),
	reorg           BOOLEAN DEFAULT FALSE,
	PRIMARY KEY (address, token_address, token_id)
);

-- address_token_id_balances
CREATE TABLE IF NOT EXISTS address_token_id_balances
(
	address         BYTEA NOT NULL,
	token_address   BYTEA NOT NULL,
	token_id        NUMERIC(78) NOT NULL,
	balance         NUMERIC(100),
	block_number    BIGINT NOT NULL,
	block_timestamp TIMESTAMP NOT NULL,
	create_time     TIMESTAMP DEFAULT NOW(),
	update_time     TIMESTAMP DEFAULT NOW(),
	reorg           BOOLEAN DEFAULT FALSE,
	PRIMARY KEY (address, token_address, token_id, block_number, block_timestamp)
);

-- token_transfers
-- erc20_token_transfers
CREATE TABLE IF NOT EXISTS erc20_token_transfers
(
    transaction_hash BYTEA NOT NULL,
    log_index        INTEGER NOT NULL,
    from_address     BYTEA,
    to_address       BYTEA,
    token_address    BYTEA,
    value            NUMERIC(100),
    block_number     BIGINT,
    block_hash       BYTEA NOT NULL,
    block_timestamp  TIMESTAMP NOT NULL,
    create_time      TIMESTAMP DEFAULT NOW(),
    update_time      TIMESTAMP DEFAULT NOW(),
    reorg            BOOLEAN,
    CONSTRAINT erc20_token_transfers_pkey
        PRIMARY KEY (transaction_hash, block_hash, log_index, block_timestamp)
) PARTITION by RANGE (block_timestamp);

SELECT partman.create_parent(
               P_PARENT_TABLE := 'public.erc20_token_transfers'::text,
               P_CONTROL := 'block_timestamp'::text,
               P_TYPE := 'range'::text,
               P_INTERVAL := :'partition_interval'::text,
               P_PREMAKE := 12::INT,
               P_AUTOMATIC_MAINTENANCE := 'on'::text,
               P_START_PARTITION := :'start_partition'::text,
               P_DEFAULT_TABLE := FALSE
       );

CREATE INDEX IF NOT EXISTS erc20_token_transfers_block_number_index
	ON erc20_token_transfers (block_number DESC);

CREATE INDEX IF NOT EXISTS erc20_token_transfers_token_address_index
	ON erc20_token_transfers (token_address DESC);

-- erc721_token_transfers
CREATE TABLE IF NOT EXISTS erc721_token_transfers
(
    transaction_hash BYTEA NOT NULL,
    log_index        INTEGER NOT NULL,
    from_address     BYTEA,
    to_address       BYTEA,
    token_address    BYTEA,
    token_id         NUMERIC(78),
    block_number     BIGINT,
    block_hash       BYTEA NOT NULL,
    block_timestamp  TIMESTAMP,
    create_time      TIMESTAMP DEFAULT NOW(),
    update_time      TIMESTAMP DEFAULT NOW(),
    reorg            BOOLEAN,
    CONSTRAINT erc721_token_transfers_pkey
        PRIMARY KEY (transaction_hash, block_hash, log_index, block_timestamp)
) PARTITION by RANGE (block_timestamp);

SELECT partman.create_parent(
               P_PARENT_TABLE := 'public.erc721_token_transfers'::text,
               P_CONTROL := 'block_timestamp'::text,
               P_TYPE := 'range'::text,
               P_INTERVAL := :'partition_interval'::text,
               P_PREMAKE := 12::INT,
               P_AUTOMATIC_MAINTENANCE := 'on'::text,
               P_START_PARTITION := :'start_partition'::text,
               P_DEFAULT_TABLE := FALSE
       );
CREATE INDEX IF NOT EXISTS erc721_token_transfers_number_log_index
    ON erc721_token_transfers (block_number DESC, log_index DESC);

CREATE INDEX IF NOT EXISTS erc721_token_transfers_token_address_id_index
    ON erc721_token_transfers (token_address, token_id, block_number DESC, log_index DESC);

CREATE INDEX IF NOT EXISTS erc721_token_transfers_token_address_number_log_index_index
    ON erc721_token_transfers (token_address ASC, block_number DESC, log_index DESC);

-- erc1155_token_transfers
CREATE TABLE IF NOT EXISTS erc1155_token_transfers
(
    transaction_hash BYTEA NOT NULL,
    log_index        INTEGER NOT NULL,
    from_address     BYTEA,
    to_address       BYTEA,
    token_address    BYTEA,
    token_id         NUMERIC(78) NOT NULL,
    value            NUMERIC(100),
    block_number     BIGINT,
    block_hash       BYTEA NOT NULL,
    block_timestamp  TIMESTAMP,
    create_time      TIMESTAMP DEFAULT NOW(),
    update_time      TIMESTAMP DEFAULT NOW(),
    reorg            BOOLEAN,
	CONSTRAINT erc1155_token_transfers_pkey
		PRIMARY KEY (transaction_hash, block_hash, log_index, block_timestamp, token_id)
) PARTITION by RANGE (block_timestamp);
SELECT partman.create_parent(
               P_PARENT_TABLE := 'public.erc1155_token_transfers'::text,
               P_CONTROL := 'block_timestamp'::text,
               P_TYPE := 'range'::text,
               P_INTERVAL := :'partition_interval'::text,
               P_PREMAKE := 12::INT,
               P_AUTOMATIC_MAINTENANCE := 'on'::text,
               P_START_PARTITION := :'start_partition'::text,
               P_DEFAULT_TABLE := FALSE
       );
CREATE INDEX IF NOT EXISTS erc1155_token_transfers_number_log_index
    ON erc1155_token_transfers (block_number DESC, log_index DESC);

CREATE INDEX IF NOT EXISTS erc1155_token_transfers_token_address_id_index
    ON erc1155_token_transfers (token_address, token_id, block_number DESC, log_index DESC);

CREATE INDEX IF NOT EXISTS erc1155_token_transfers_token_address_number_log_index_index
    ON erc1155_token_transfers (token_address ASC, block_number DESC, log_index DESC);

-- nft
CREATE TABLE IF NOT EXISTS nft_details
(
    token_address  BYTEA NOT NULL,
    token_id       NUMERIC(100) NOT NULL,
    token_supply   NUMERIC(78),
    token_owner    BYTEA,
    token_uri      VARCHAR,
    token_uri_info JSONB,
    block_number   BIGINT,
    block_timestamp TIMESTAMP,
    create_time    TIMESTAMP DEFAULT NOW(),
    update_time    TIMESTAMP DEFAULT NOW(),
    reorg          BOOLEAN DEFAULT FALSE,
    PRIMARY KEY (token_address, token_id)
);

CREATE INDEX IF NOT EXISTS nft_details_token_address_index
    ON nft_details (token_address DESC, token_id);

CREATE INDEX IF NOT EXISTS nft_details_address_index
    ON nft_details (token_owner DESC, token_id ASC);

CREATE TABLE IF NOT EXISTS nft_id_changes
(
    token_address  BYTEA NOT NULL,
    token_id       NUMERIC(100) NOT NULL,
    token_owner    BYTEA,
    block_number   BIGINT NOT NULL,
    block_timestamp TIMESTAMP,
    create_time    TIMESTAMP DEFAULT NOW(),
    update_time    TIMESTAMP DEFAULT NOW(),
    reorg          BOOLEAN DEFAULT FALSE,
    PRIMARY KEY (token_address, token_id, block_number)
);

CREATE INDEX IF NOT EXISTS nft_id_number_desc_index
    ON nft_id_changes (token_address, token_id, block_number DESC);

-- nft_transfers
CREATE TABLE IF NOT EXISTS nft_transfers (
    transaction_hash BYTEA NOT NULL,
    block_hash BYTEA NOT NULL,
    log_index INTEGER NOT NULL,
    token_id NUMERIC(100) NOT NULL,
    block_timestamp TIMESTAMP NOT NULL,
    block_number INTEGER NOT NULL,

    from_address BYTEA,
    to_address BYTEA,
    token_address BYTEA,
    value NUMERIC(100),

    create_time TIMESTAMP DEFAULT now(),
    update_time TIMESTAMP DEFAULT now(),
    reorg BOOLEAN DEFAULT FALSE,

    PRIMARY KEY (
        transaction_hash,
        block_hash,
        log_index,
        token_id,
        block_timestamp,
        block_number
    )
) PARTITION by RANGE (block_timestamp);
SELECT partman.create_parent(
			 P_PARENT_TABLE := 'public.nft_transfers'::text,
			 P_CONTROL := 'block_timestamp'::text,
			 P_TYPE := 'range'::text,
			 P_INTERVAL := :'partition_interval'::text,
			 P_PREMAKE := 12::INT,
			 P_AUTOMATIC_MAINTENANCE := 'on'::text,
			 P_START_PARTITION := :'start_partition'::text,
			 P_DEFAULT_TABLE := FALSE
	 );

CREATE INDEX idx_nft_transfers_block_log
    ON nft_transfers (block_number DESC, log_index DESC);

CREATE INDEX idx_nft_transfers_token_time
    ON nft_transfers (token_address, block_number DESC, log_index DESC);

CREATE INDEX idx_nft_transfers_token_id
    ON nft_transfers (token_address, token_id, block_number DESC, log_index DESC);
