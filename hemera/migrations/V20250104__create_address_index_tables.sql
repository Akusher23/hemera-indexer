\set start_partition '2025-01-01'
\set partition_interval '1 month'

-- address_transactions
CREATE TABLE IF NOT EXISTS address_transactions
(
    address             BYTEA NOT NULL,
    block_number        BIGINT NOT NULL,
    transaction_index   INTEGER NOT NULL,
    block_timestamp     TIMESTAMP NOT NULL,
    transaction_hash    BYTEA,
    block_hash          BYTEA,
    txn_type            SMALLINT,
    related_address     BYTEA,
    value               NUMERIC(100),
    transaction_fee     NUMERIC(100),
    receipt_status      INTEGER,
    method              VARCHAR,
    create_time         TIMESTAMP DEFAULT NOW(),
    update_time         TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (address, block_timestamp, block_number, transaction_index, block_hash)
) PARTITION BY RANGE (block_timestamp);

SELECT partman.create_parent(
			 P_PARENT_TABLE := 'public.address_transactions'::text,
			 P_CONTROL := 'block_timestamp'::text,
			 P_TYPE := 'range'::text,
			 P_INTERVAL := :'partition_interval'::text,
			 P_PREMAKE := 12::INT,
			 P_AUTOMATIC_MAINTENANCE := 'on'::text,
			 P_START_PARTITION := :'start_partition'::text,
			 P_DEFAULT_TABLE := FALSE
	 );

CREATE INDEX IF NOT EXISTS address_transactions_address_block_timestamp_block_number_t_idx
    ON address_transactions (address, block_number DESC, transaction_index DESC);

CREATE INDEX IF NOT EXISTS address_transactions_address_txn_type_block_timestamp_block_idx
    ON address_transactions (address, txn_type, block_number DESC, transaction_index DESC);

CREATE INDEX IF NOT EXISTS address_transactions_block_hash_idx
	ON address_transactions (block_hash);

-- address_token_transfers
CREATE TABLE IF NOT EXISTS address_token_transfers
(
    address            BYTEA NOT NULL,
    block_number       BIGINT NOT NULL,
    log_index          INTEGER NOT NULL,
    transaction_hash   BYTEA NOT NULL,
    block_timestamp    TIMESTAMP NOT NULL,
    block_hash         BYTEA NOT NULL,
    token_address      BYTEA,
    related_address    BYTEA,
    transfer_type      INTEGER,
    value              NUMERIC(100),
    create_time        TIMESTAMP DEFAULT NOW(),
    update_time        TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (address, block_timestamp, block_number, log_index, transaction_hash, block_hash)
) PARTITION BY RANGE (block_timestamp);

SELECT partman.create_parent(
			 P_PARENT_TABLE := 'public.address_token_transfers'::text,
			 P_CONTROL := 'block_timestamp'::text,
			 P_TYPE := 'range'::text,
			 P_INTERVAL := :'partition_interval'::text,
			 P_PREMAKE := 12::INT,
			 P_AUTOMATIC_MAINTENANCE := 'on'::text,
			 P_START_PARTITION := :'start_partition'::text,
			 P_DEFAULT_TABLE := FALSE
	 );

CREATE INDEX IF NOT EXISTS address_token_transfers_wallet_address_token_address__idx
    ON address_token_transfers (address, block_number DESC, log_index DESC);

CREATE INDEX IF NOT EXISTS address_token_transfers_txn_type_block_timestamp_block_idx
    ON address_token_transfers (address, transfer_type, block_number DESC, log_index DESC);

CREATE INDEX IF NOT EXISTS address_token_transfers_block_hash_idx
    ON address_token_transfers (block_hash);


CREATE TABLE IF NOT EXISTS address_nft_transfers
(
    address           BYTEA NOT NULL,
    block_number      BIGINT NOT NULL,
    log_index         INTEGER NOT NULL,
    transaction_hash  BYTEA NOT NULL,
    block_timestamp   TIMESTAMP NOT NULL,
    token_id          NUMERIC(100) NOT NULL,
    block_hash        BYTEA,
    token_address     BYTEA,
    related_address   BYTEA,
    transfer_type     INTEGER,
    value             NUMERIC(100),
    create_time       TIMESTAMP DEFAULT NOW(),
    update_time       TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (address, block_number, log_index, transaction_hash, block_timestamp, token_id, block_hash)
)  PARTITION BY RANGE (block_timestamp);

SELECT partman.create_parent(
               P_PARENT_TABLE := 'public.address_nft_transfers'::text,
               P_CONTROL := 'block_timestamp'::text,
               P_TYPE := 'range'::text,
               P_INTERVAL := :'partition_interval'::text,
               P_PREMAKE := 12::INT,
               P_AUTOMATIC_MAINTENANCE := 'on'::text,
               P_START_PARTITION := :'start_partition'::text,
               P_DEFAULT_TABLE := FALSE
       );

CREATE INDEX IF NOT EXISTS idx_address_nft_transfers_token_time
    ON address_nft_transfers (address, block_timestamp DESC, block_number DESC, log_index DESC);

CREATE INDEX IF NOT EXISTS address_nft_transfers_block_hash_idx
    ON address_token_transfers (block_hash);