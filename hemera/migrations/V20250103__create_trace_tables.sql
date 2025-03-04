\set start_partition '2025-01-01'
\set partition_interval '1 month'

-- traces
CREATE TABLE IF NOT EXISTS traces
(
    trace_id        TEXT NOT NULL,
    block_timestamp TIMESTAMP NOT NULL,
    from_address    BYTEA,
    to_address      BYTEA,
    value           NUMERIC(100),
    input           BYTEA,
    output          BYTEA,
    trace_type      VARCHAR,
    call_type       VARCHAR,
    gas             NUMERIC(100),
    gas_used        NUMERIC(100),
    subtraces       INTEGER,
    trace_address   INTEGER[],
    error           VARCHAR,
    status          INTEGER,
    block_number    BIGINT,
    block_hash      BYTEA,
    transaction_index INTEGER,
    transaction_hash  BYTEA,
    create_time     TIMESTAMP DEFAULT NOW(),
    update_time     TIMESTAMP DEFAULT NOW(),
    reorg           BOOLEAN DEFAULT FALSE,
    PRIMARY KEY (trace_id, block_timestamp)
) PARTITION BY RANGE (block_timestamp);

SELECT partman.create_parent(
			 P_PARENT_TABLE := 'public.traces'::text,
			 P_CONTROL := 'block_timestamp'::text,
			 P_TYPE := 'range'::text,
			 P_INTERVAL := :'partition_interval'::text,
			 P_PREMAKE := 12::INT,
			 P_AUTOMATIC_MAINTENANCE := 'on'::text,
			 P_START_PARTITION := :'start_partition'::text,
			 P_DEFAULT_TABLE := FALSE
	 );

CREATE INDEX IF NOT EXISTS traces_transaction_hash_index
    ON traces (transaction_hash);

CREATE INDEX IF NOT EXISTS traces_block_number_index
    ON traces (block_number DESC);

-- contract_internal_transactions
CREATE TABLE IF NOT EXISTS contract_internal_transactions
(
    trace_id        TEXT NOT NULL,
    block_timestamp TIMESTAMP NOT NULL,
    from_address    BYTEA,
    to_address      BYTEA,
    value           NUMERIC(100),
    input           BYTEA,
    output          BYTEA,
    trace_type      VARCHAR,
    call_type       VARCHAR,
    gas             NUMERIC(100),
    gas_used        NUMERIC(100),
    subtraces       INTEGER,
    trace_address   INTEGER[],
    error           VARCHAR,
    status          INTEGER,
    block_number    BIGINT,
    block_hash      BYTEA,
    transaction_index INTEGER,
    transaction_hash  BYTEA,
    create_time     TIMESTAMP DEFAULT NOW(),
    update_time     TIMESTAMP DEFAULT NOW(),
    reorg           BOOLEAN DEFAULT FALSE,
    PRIMARY KEY (trace_id, block_timestamp)
) PARTITION BY RANGE (block_timestamp);

SELECT partman.create_parent(
		 P_PARENT_TABLE := 'public.contract_internal_transactions'::text,
		 P_CONTROL := 'block_timestamp'::text,
		 P_TYPE := 'range'::text,
		 P_INTERVAL := :'partition_interval'::text,
		 P_PREMAKE := 12::INT,
		 P_AUTOMATIC_MAINTENANCE := 'on'::text,
		 P_START_PARTITION := :'start_partition'::text,
		 P_DEFAULT_TABLE := FALSE
 );

CREATE INDEX IF NOT EXISTS contract_internal_transactions_transaction_hash_index
    ON contract_internal_transactions (transaction_hash);

CREATE INDEX IF NOT EXISTS contract_internal_transactions_block_number_index
    ON contract_internal_transactions (block_number DESC);

-- transaction_trace_json
CREATE TABLE IF NOT EXISTS transaction_trace_json
(
    transaction_hash BYTEA NOT NULL,
    block_timestamp  TIMESTAMP NOT NULL,
    block_number     BIGINT,
    block_hash       BYTEA,
    data             JSONB,
    PRIMARY KEY (transaction_hash, block_timestamp)
) PARTITION BY RANGE (block_timestamp);

SELECT partman.create_parent(
		 P_PARENT_TABLE := 'public.transaction_trace_json'::text,
		 P_CONTROL := 'block_timestamp'::text,
		 P_TYPE := 'range'::text,
		 P_INTERVAL := :'partition_interval'::text,
		 P_PREMAKE := 12::INT,
		 P_AUTOMATIC_MAINTENANCE := 'on'::text,
		 P_START_PARTITION := :'start_partition'::text,
		 P_DEFAULT_TABLE := FALSE
 );

-- address_coin_balances
CREATE TABLE IF NOT EXISTS address_coin_balances
(
	address         BYTEA NOT NULL,
	block_number    BIGINT NOT NULL,
	block_timestamp TIMESTAMP NOT NULL,
	balance         NUMERIC(100),
	create_time     TIMESTAMP DEFAULT NOW(),
	update_time     TIMESTAMP DEFAULT NOW(),
	reorg           BOOLEAN DEFAULT FALSE,
	PRIMARY KEY (address, block_number, block_timestamp)
)  PARTITION BY RANGE (block_timestamp);

SELECT partman.create_parent(
	 P_PARENT_TABLE := 'public.address_coin_balances'::text,
	 P_CONTROL := 'block_timestamp'::text,
	 P_TYPE := 'range'::text,
	 P_INTERVAL := :'partition_interval'::text,
	 P_PREMAKE := 12::INT,
	 P_AUTOMATIC_MAINTENANCE := 'on'::text,
	 P_START_PARTITION := :'start_partition'::text,
	 P_DEFAULT_TABLE := FALSE
);

-- contracts
CREATE TABLE contracts
(
    address                          BYTEA NOT NULL
        PRIMARY KEY,
    name                             VARCHAR,
    contract_creator                 BYTEA,
    creation_code                    BYTEA,
    deployed_code                    BYTEA,
    block_number                     BIGINT,
    block_hash                       BYTEA,
    block_timestamp                  TIMESTAMP,
    transaction_index                INTEGER,
    transaction_hash                 BYTEA,
    official_website                 VARCHAR,
    description                      VARCHAR,
    email                            VARCHAR,
    social_list                      JSONB,
    is_verified                      BOOLEAN,
    is_proxy                         BOOLEAN,
    implementation_contract          BYTEA,
    verified_implementation_contract BYTEA,
    proxy_standard                   VARCHAR,
    create_time                      TIMESTAMP DEFAULT NOW(),
    update_time                      TIMESTAMP DEFAULT NOW(),
    reorg                            BOOLEAN DEFAULT FALSE,
    deployed_code_hash               VARCHAR GENERATED ALWAYS AS (ENCODE(
            DIGEST(('0x'::TEXT || ENCODE(deployed_code, 'hex'::TEXT)), 'sha256'::TEXT), 'hex'::TEXT)) STORED,
    transaction_from_address         BYTEA,
    bytecode                         VARCHAR GENERATED ALWAYS AS (('0x'::text || ENCODE(creation_code, 'hex'::TEXT))) STORED
);
