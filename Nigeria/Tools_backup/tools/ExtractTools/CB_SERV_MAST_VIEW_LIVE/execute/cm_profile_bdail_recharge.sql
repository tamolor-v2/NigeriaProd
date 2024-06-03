start transaction;
delete from nigeria.cm_profile_bdail_recharge where tbl_dt=20190108;
insert into nigeria.cm_profile_bdail_recharge
select 
msisdn_key,
try_cast(avg(transaction_amt) as int) average_refill_amount,
try_cast(max(transaction_amt) as int) max_refill_amount,
tbl_dt
from flare_8.cs5_air_refill_ma where tbl_dt=20190108
and tbl_dt between 
cast(date_format(date_add('day',-29,date_parse(cast(20190108 as varchar),'%Y%m%d')),'%Y%m%d') as int) 
and 20190108 
and transaction_amt>0
group by tbl_dt,msisdn_key;
commit;