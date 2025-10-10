begin;
delete
from daily_feature_erc20_token_supply_records
WHERE block_date >= '{start_date}'
  and block_date < '{end_date}';

insert into public.daily_feature_erc20_token_supply_records(block_date, token_address, total_supply)
select date(block_timestamp)::date,
       token_address,
       total_supply
from (select *,
             row_number() over (partition by token_address order by block_timestamp desc) as rn
      from af_erc20_total_supply_hist
      WHERE block_timestamp >= '{start_date}'
        and block_timestamp < '{end_date}') t
where rn = 1;

commit