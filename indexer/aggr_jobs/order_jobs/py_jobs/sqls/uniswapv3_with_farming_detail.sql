with farming_config as (select decode('827922686190790b37229fd06084350e74485b72', 'hex') as token_address,
                               decode('d19B0811e28990dfF5D3C4B7F2AbC681c21a7c0E', 'hex') as to_address),
     farming_transfer_table as (select wallet_address, token_address, token_id
                                from (select from_address as                                                                         wallet_address,
                                             d1.token_address,
                                             token_id,
                                             row_number()
                                             over (partition by from_address, d1.token_address, token_id order by block_number desc) rn
                                      from erc721_token_transfers d1
                                               inner join farming_config d2
                                                          on d1.token_address = d2.token_address and d1.to_address = d2.to_address
                                      where block_timestamp < date('{start_date}') + interval '1 days') t
                                where rn = 1),


     all_detail_table as (select d1.period_date,
                                 d3.position_token_address as nft_address,
                                 d3.token_id,
                                 wallet_address,
                                 d1.pool_address           as contract_address,
                                 liquidity,
                                 tick_upper,
                                 tick_lower,
                                 sqrt_price_x96,
                                 token0_address,
                                 token1_address,
                                 d5.decimals               AS token0_decimals,
                                 d5.symbol                 AS token0_symbol,
                                 d6.decimals               AS token1_decimals,
                                 d6.symbol                 AS token1_symbol
                          from (select * from af_uniswap_v3_pool_prices_period where period_date = '{start_date}') d1
                                   inner join af_uniswap_v3_pools d2
                                              on d1.pool_address = d2.pool_address
                                   inner join (select *
                                               from af_uniswap_v3_token_data_period
                                               where period_date = '{start_date}') d3
                                              on d1.pool_address = d3.pool_address
                                   inner join af_uniswap_v3_tokens d4
                                              ON d3.position_token_address = d4.position_token_address
                                                  AND d3.token_id = d4.token_id
                                   INNER JOIN tokens d5 ON d2.token0_address = d5.address
                                   INNER JOIN tokens d6 ON d2.token1_address = d6.address
                              and d5.symbol is not null and d6.symbol is not null
                              and (d5.symbol = 'FBTC' or d6.symbol = 'FBTC')),

     farming_detail as (select d1.period_date,
                               d1.nft_address,
                               d1.token_id,
                               coalesce(d2.wallet_address, d1.wallet_address) as wallet_address,
                               d1.contract_address,
                               d1.liquidity,
                               d1.tick_upper,
                               d1.tick_lower,
                               d1.sqrt_price_x96,
                               d1.token0_address,
                               d1.token1_address,
                               d1.token0_decimals,
                               d1.token0_symbol,
                               d1.token1_decimals,
                               d1.token1_symbol
                        from all_detail_table d1
                                 inner join farming_transfer_table d2
                                            on d1.nft_address = d2.token_address and d1.token_id = d2.token_id),

     liquidity_detail as (SELECT d1.*
                          FROM all_detail_table d1
                                   LEFT JOIN farming_transfer_table d2
                                             ON d1.nft_address = d2.token_address
                                                 AND d1.token_id = d2.token_id
                          WHERE d2.token_address IS NULL)

select *
from farming_detail
union all
select *
from liquidity_detail;