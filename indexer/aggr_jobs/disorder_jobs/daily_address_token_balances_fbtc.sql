delete
from daily_address_token_balances
WHERE block_date >= '{start_date}'
  and block_date < '{end_date}';

insert into public.daily_address_token_balances (address, block_date, token_address, token_id, token_type, balance
                                                 )

select address,
       date(block_timestamp) as block_date,
       token_address,
       null as token_id,
       token_type,
       balance
from (select *,
             row_number() over (partition by address,token_address,token_id order by block_timestamp desc) as rn
      from address_token_balances
      WHERE block_timestamp >= '{start_date}'
        and block_timestamp < '{end_date}'
        and token_address = '\xc96de26018a54d51c097160568752c4e3bd6c364'
      ) t
where rn = 1;

