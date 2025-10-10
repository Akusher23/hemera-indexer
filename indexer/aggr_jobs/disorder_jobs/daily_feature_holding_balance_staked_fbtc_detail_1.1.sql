delete
from daily_feature_holding_balance_staked_fbtc_detail
where block_date >= '{start_date}'
  and block_date < '{end_date}';
insert into public.daily_feature_holding_balance_staked_fbtc_detail (block_date, wallet_address, protocol_id,
                                                                     contract_address, balance)
select TO_TIMESTAMP(block_timestamp)::DATE as block_date,
       wallet_address,
       protocol_id,
       contract_address,
       block_cumulative_value
from (select *,
             row_number()
             over (partition by contract_address, protocol_id, wallet_address order by block_timestamp desc) rn
      from af_staked_transferred_balance_hist
      where TO_TIMESTAMP(block_timestamp) >= '{start_date}'
        and TO_TIMESTAMP(block_timestamp) < '{end_date}'
            AND token_address = '\xc96de26018a54d51c097160568752c4e3bd6c364'
      ) t
where rn = 1;


