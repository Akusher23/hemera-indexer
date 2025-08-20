with user_yt_balance_table as (select address, balance / pow(10, 18) as yt_balance
                               from period_address_token_balances
                               where token_address = '\x6bd129974d12d3c3efe1cce95a9bc822d811033c'),

     user_lp_balance_table as (select address,
                                      balance / pow(10, 18)          as lp_balance,
                                      balance / sum(balance) over () as lp_ratio
                               from period_address_token_balances
                               where token_address = '\x1dc93df5d77b705c8c16527ec800961f1a7b3413'),
     lp_balance_table as (select t0.balance / pow(10, 18) as token0_balance,
                                 t1.balance / pow(10, 18) as token1_balance
                          from (select balance
                                from period_address_token_balances
                                where address = '\x1dc93df5d77b705c8c16527ec800961f1a7b3413'
                                  and token_address = '\xB74E4F4ADD805A7191A934A05D3A826E8D714A44'
                                limit 1) t0
                                   cross join (select balance
                                               from period_address_token_balances
                                               where address = '\x1dc93df5d77b705c8c16527ec800961f1a7b3413'
                                                 and token_address = '\x40DEFB4B2A451C7BAD7C256132085AC4348C3B4C'
                                               limit 1) t1)

select coalesce(yt.address, lp.address)  as address,
       yt.yt_balance,
       lp.lp_balance,
       pool.token0_balance * lp.lp_ratio as lp_yt_balance,
       pool.token1_balance * lp.lp_ratio as lp_pt_balance
from user_yt_balance_table yt
         full join user_lp_balance_table lp
                   on yt.address = lp.address
         cross join lp_balance_table pool;
