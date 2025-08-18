delete
from period_feature_merchant_moe_token_bin_records
where period_date >= '{start_date}'
  and period_date < '{end_date}';

insert into period_feature_merchant_moe_token_bin_records(period_date, token_address, token_id, reserve0_bin, reserve1_bin)
select date('{start_date}'), token_address, token_id, reserve0_bin, reserve1_bin
from (select *, row_number() over (partition by token_address, token_id order by block_timestamp desc) as rn
      from feature_merchant_moe_token_bin_records
      where to_timestamp(block_timestamp) < '{end_date}') t
where rn = 1
;


delete
from period_feature_holding_balance_merchantmoe
where period_date >= '{start_date}'
  and period_date < '{end_date}';
insert
into period_feature_holding_balance_merchantmoe(period_date, protocol_id, contract_address, token_id,
                                                wallet_address, token0_address, token0_symbol, token0_balance,
                                                token1_address, token1_symbol, token1_balance)
WITH day_tokens AS (SELECT s.token_address,
                           s.token_id,
                           s.total_supply,
                           b.reserve0_bin,
                           b.reserve1_bin
                    FROM period_feature_erc1155_token_supply_records s
                             JOIN period_feature_merchant_moe_token_bin_records b
                                  ON b.period_date = s.period_date
                                      AND b.token_address = s.token_address
                                      AND b.token_id = s.token_id
                    WHERE s.period_date = DATE '{start_date}'),
     erc1155_balances AS (SELECT address, token_address, token_id, balance
                          FROM period_address_token_balances
                          WHERE token_type = 'ERC1155'
                            and balance > 0),
     pools_token0_fbtc AS (SELECT p.token_address,
                                   p.token0_address,
                                   t0.symbol   AS token0_symbol,
                                   t0.decimals AS token0_decimals,
                                   p.token1_address,
                                   t1.symbol   AS token1_symbol,
                                   t1.decimals AS token1_decimals
                            FROM feature_merchant_moe_pools p
                                     JOIN tokens t0 ON t0.address = p.token0_address
                                     JOIN tokens t1 ON t1.address = p.token1_address
                            WHERE t0.symbol = 'FBTC'),
     pools_token1_fbtc AS (SELECT p.token_address,
                                   p.token0_address,
                                   t0.symbol   AS token0_symbol,
                                   t0.decimals AS token0_decimals,
                                   p.token1_address,
                                   t1.symbol   AS token1_symbol,
                                   t1.decimals AS token1_decimals
                            FROM feature_merchant_moe_pools p
                                     JOIN tokens t0 ON t0.address = p.token0_address
                                     JOIN tokens t1 ON t1.address = p.token1_address
                            WHERE t1.symbol = 'FBTC'),
     pools_fbtc AS (SELECT *
                     FROM pools_token0_fbtc
                     UNION ALL
                     SELECT *
                     FROM pools_token1_fbtc)


SELECT DATE '{start_date}' AS period_date,
       'merchantmoe'       AS protocol_id,
       b.token_address     AS nft_address,
       b.token_id,
       b.address,
       pc.token0_address,
       pc.token0_symbol,
       CASE
           WHEN dt.total_supply > 0
               THEN (b.balance::double precision / dt.total_supply::double precision)
                        * dt.reserve0_bin::double precision
               / pow(10, pc.token0_decimals)
           ELSE 0 END      AS token0_balance,
       pc.token1_address,
       pc.token1_symbol,
       CASE
           WHEN dt.total_supply > 0
               THEN (b.balance::double precision / dt.total_supply::double precision)
                        * dt.reserve1_bin::double precision
               / pow(10, pc.token1_decimals)
           ELSE 0 END      AS token1_balance
FROM day_tokens dt
         JOIN erc1155_balances b
              ON b.token_address = dt.token_address AND b.token_id = dt.token_id
         JOIN pools_fbtc pc
              ON pc.token_address = dt.token_address;

