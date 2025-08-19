delete
from period_feature_merchant_moe_token_bin_records
where period_date >= '{start_date}'
  and period_date < '{end_date}';

insert into period_feature_merchant_moe_token_bin_records(period_date, token_address, token_id, reserve0_bin, reserve1_bin)
select date('{start_date}'), token_address, token_id, reserve0_bin, reserve1_bin
from (select *, row_number() over (partition by token_address, token_id order by block_timestamp desc) as rn
          from feature_merchant_moe_token_bin_records
where block_timestamp < extract(epoch from timestamp '{end_date}')) t
where rn = 1
;


delete
from period_feature_holding_balance_merchantmoe
where period_date >= '{start_date}'
  and period_date < '{end_date}';
BEGIN;

-- 1) Moe 池 + token 元数据 -> 落地
DROP TABLE IF EXISTS t_moe_pools;
CREATE TEMP TABLE t_moe_pools AS
SELECT d0.*,
       d4.symbol   AS token0_symbol,
       d4.decimals AS token0_decimals,
       d5.symbol   AS token1_symbol,
       d5.decimals AS token1_decimals
FROM feature_merchant_moe_pools d0
JOIN tokens d4 ON d0.token0_address = d4.address
JOIN tokens d5 ON d0.token1_address = d5.address
WHERE d4.symbol = 'FBTC' OR d5.symbol = 'FBTC';

CREATE INDEX ON t_moe_pools (token_address);
CREATE INDEX ON t_moe_pools (token0_address);
CREATE INDEX ON t_moe_pools (token1_address);
ANALYZE t_moe_pools;

-- 2) ERC1155 余额 -> 落地
DROP TABLE IF EXISTS t_balances;
CREATE TEMP TABLE t_balances AS
SELECT *
FROM period_address_token_balances
WHERE token_type = 'ERC1155' AND balance > 0;

CREATE INDEX ON t_balances (token_address, token_id);
CREATE INDEX ON t_balances (address);
ANALYZE t_balances;

-- 3) moe 池与余额记录 -> 落地
DROP TABLE IF EXISTS t_pool_records;
CREATE TEMP TABLE t_pool_records AS
SELECT p.*,
       b.address,
       b.token_id,
       b.balance
FROM t_moe_pools p
JOIN t_balances b
  ON p.token_address = b.token_address;

CREATE INDEX ON t_pool_records (token_address, token_id);
CREATE INDEX ON t_pool_records (address);
ANALYZE t_pool_records;

-- 4) ERC1155 supply（按日） -> 落地
DROP TABLE IF EXISTS t_supply;
CREATE TEMP TABLE t_supply AS
SELECT token_address, token_id, total_supply
FROM period_feature_erc1155_token_supply_records
WHERE period_date = '{start_date}';

CREATE INDEX ON t_supply (token_address, token_id);
ANALYZE t_supply;

-- 5) bin 储备（按日） -> 落地
DROP TABLE IF EXISTS t_bins;
CREATE TEMP TABLE t_bins AS
SELECT token_address, token_id, reserve0_bin, reserve1_bin
FROM period_feature_merchant_moe_token_bin_records
WHERE period_date = '{start_date}';

CREATE INDEX ON t_bins (token_address, token_id);
ANALYZE t_bins;

-- 6) detail -> 落地
DROP TABLE IF EXISTS t_detail;
CREATE TEMP TABLE t_detail AS
SELECT r.address,
       r.token_address,
       r.token_id,
       r.balance,
       s.total_supply,
       b.reserve0_bin,
       b.reserve1_bin,
       r.token0_address,
       r.token0_symbol,
       r.token0_decimals,
       r.token1_address,
       r.token1_symbol,
       r.token1_decimals
FROM t_pool_records r
JOIN t_supply s USING (token_address, token_id)
JOIN t_bins   b USING (token_address, token_id);

CREATE INDEX ON t_detail (token_address, token_id);
CREATE INDEX ON t_detail (address);
ANALYZE t_detail;

-- 7) 最终写入目标表
INSERT INTO period_feature_holding_balance_merchantmoe (
  period_date, protocol_id, contract_address, token_id,
  wallet_address, token0_address, token0_symbol, token0_balance,
  token1_address, token1_symbol, token1_balance
)
SELECT DATE('{start_date}')::date,
       'merchantmoe'                       AS protocol_id,
       token_address                       AS contract_address,
       token_id,
       address                             AS wallet_address,
       token0_address,
       token0_symbol,
       CASE
         WHEN total_supply > 0
         THEN (balance::numeric / total_supply) * reserve0_bin / pow(10, token0_decimals)
         ELSE 0
       END AS token0_balance,
       token1_address,
       token1_symbol,
       CASE
         WHEN total_supply > 0
         THEN (balance::numeric / total_supply) * reserve1_bin / pow(10, token1_decimals)
         ELSE 0
       END AS token1_balance
FROM t_detail;

COMMIT;


