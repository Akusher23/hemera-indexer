\set start_partition '2025-01-01'
\set partition_interval '1 month'

-- blocks
CREATE TABLE IF NOT EXISTS blocks
(
    hash                        BYTEA not null
        primary key,
    number                      BIGINT,
    timestamp                   timestamp,
    parent_hash                 BYTEA,
    nonce                       BYTEA,
    gas_limit                   NUMERIC(100),
    gas_used                    NUMERIC(100),
    base_fee_per_gas            NUMERIC(100),
    difficulty                  NUMERIC(38),
    total_difficulty            NUMERIC(38),
    size                        BIGINT,
    miner                       BYTEA,
    sha3_uncles                 BYTEA,
    transactions_root           BYTEA,
    transactions_count          BIGINT,
    state_root                  BYTEA,
    receipts_root               BYTEA,
    extra_data                  BYTEA,
    withdrawals_root            BYTEA,
    create_time                 TIMESTAMP default now(),
    update_time                 TIMESTAMP default now(),
    reorg                       BOOLEAN,
    blob_gas_used               NUMERIC(100),
    excess_blob_gas             NUMERIC(100),
    traces_count                BIGINT,
    internal_transactions_count BIGINT
);

CREATE INDEX IF NOT EXISTS blocks_number_index
    ON blocks (number DESC);

CREATE INDEX IF NOT EXISTS blocks_timestamp_index
    ON blocks (TIMESTAMP DESC);

CREATE UNIQUE INDEX IF NOT EXISTS  blocks_hash_unique_when_not_reorg
    ON blocks (hash)
    WHERE (reorg = FALSE);

CREATE UNIQUE index blocks_number_unique_when_not_reorg
    ON blocks (number)
    WHERE (reorg = FALSE);

-- transactions
CREATE TABLE IF NOT EXISTS transactions
(
    hash                        BYTEA NOT NULL,
    transaction_index           INTEGER,
    from_address                BYTEA,
    to_address                  BYTEA,
    value                       NUMERIC(100),
    transaction_type            INTEGER,
    input                       BYTEA,
    nonce                       NUMERIC(100),
    block_hash                  BYTEA,
    block_number                BIGINT,
    block_timestamp             TIMESTAMP,
    gas                         NUMERIC(100),
    gas_price                   NUMERIC(100),
    max_fee_per_gas             NUMERIC(100),
    max_priority_fee_per_gas    NUMERIC(100),
    receipt_root                BYTEA,
    receipt_status              INTEGER,
    receipt_gas_used            NUMERIC(100),
    receipt_cumulative_gas_used NUMERIC(100),
    receipt_effective_gas_price NUMERIC(100),
    receipt_l1_fee              NUMERIC(100),
    receipt_l1_fee_scalar       NUMERIC(100, 18),
    receipt_l1_gas_used         NUMERIC(100),
    receipt_l1_gas_price        NUMERIC(100),
    receipt_blob_gas_used       NUMERIC(100),
    receipt_blob_gas_price      NUMERIC(100),
    blob_versioned_hashes       BYTEA[],
    receipt_contract_address    BYTEA,
    exist_error                 BOOLEAN,
    error                       TEXT,
    revert_reason               TEXT,
    create_time                 TIMESTAMP DEFAULT NOW(),
    update_time                 TIMESTAMP DEFAULT NOW(),
    reorg                       BOOLEAN DEFAULT FALSE,
    method_id                   VARCHAR GENERATED ALWAYS AS (SUBSTR(((input)::character varying)::text, 3, 8)) STORED,
    PRIMARY KEY (hash, block_timestamp)
) PARTITION BY RANGE (block_timestamp);


CREATE INDEX IF NOT EXISTS transactions_block_number_transaction_index
    ON transactions (block_number DESC, transaction_index DESC);

CREATE INDEX IF NOT EXISTS transactions_block_timestamp_index
    ON transactions (block_timestamp DESC);

SELECT partman.create_parent(
               p_parent_table := 'public.transactions'::text,
                p_control := 'block_timestamp'::text,
                p_type := 'range'::text,
                p_interval := :'partition_interval'::text,
                p_premake := 12::int,
                p_automatic_maintenance := 'on'::text,
                p_start_partition := :'start_partition'::text,
                p_default_table := false
            );

-- logs
CREATE TABLE IF NOT EXISTS logs
(
    log_index         INTEGER NOT NULL,
    address           BYTEA,
    data              BYTEA,
    topic0            BYTEA,
    topic1            BYTEA,
    topic2            BYTEA,
    topic3            BYTEA,
    transaction_hash  BYTEA NOT NULL,
    transaction_index INTEGER,
    block_number      BIGINT,
    block_hash        BYTEA NOT NULL,
    block_timestamp   TIMESTAMP,
    create_time       TIMESTAMP DEFAULT NOW(),
    update_time       TIMESTAMP DEFAULT NOW(),
    reorg             BOOLEAN DEFAULT FALSE,
    PRIMARY KEY (transaction_hash, block_hash, log_index, block_timestamp)
) PARTITION BY RANGE (block_timestamp);

CREATE INDEX IF NOT EXISTS logs_address_block_number_log_index_index
    ON logs (address ASC, block_number DESC, log_index DESC);

CREATE INDEX IF NOT EXISTS logs_block_timestamp_index
    ON logs (block_timestamp DESC);

CREATE INDEX IF NOT EXISTS logs_address_topic_0_number_log_index_index
    ON logs (address ASC, topic0 ASC, block_number DESC, log_index DESC);

CREATE INDEX IF NOT EXISTS logs_block_number_log_index_index
    ON logs (block_number DESC, log_index DESC);

SELECT partman.create_parent(
               p_parent_table := 'public.logs'::text,
               p_control := 'block_timestamp'::text,
               p_type := 'range'::text,
               p_interval := :'partition_interval'::text,
               p_premake := 12::INT,
               p_automatic_maintenance := 'on'::text,
               p_start_partition := :'start_partition'::text,
               p_default_table := FALSE
            );

CREATE TABLE block_ts_mapper
(
    ts           BIGSERIAL
        PRIMARY KEY,
    block_number BIGINT,
    timestamp    TIMESTAMP
);

CREATE INDEX block_ts_mapper_block_number_idx
    ON block_ts_mapper (block_number DESC);
