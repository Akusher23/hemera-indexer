with user_yt_balance_table as (select address, balance / pow(10, 18) as yt_balance
                               from
period_address_token_balances
                               where token_address = '\x6bd129974d12d3c3efe1cce95a9bc822d811033c'),

     user_lp_balance_table as (select address,
                                      balance / sum(balance) over () as lp_ratio
                               from period_address_token_balances
                               where token_address = '\x1dc93df5d77b705c8c16527ec800961f1a7b3413'),
     lp_balance_table as (select token0_balance / pow(10, 18) as token0_balance,
                                 token1_balance / pow(10, 18) as token1_balance
                          from af_spectra_lp_balance
                          where block_timestamp < date('{start_date}') + interval '1 days'
                          order by block_timestamp desc
                          limit 1)


select coalesce(yt.address, lp.address)  as address,
       yt.yt_balance,
       lp.lp_ratio,
       pool.token0_balance * lp.lp_ratio as lp_yt_balance,
       pool.token1_balance * lp.lp_ratio as lp_pt_balance
from user_yt_balance_table yt
         full join user_lp_balance_table lp
                   on yt.address = lp.address
         cross join lp_balance_table pool;
