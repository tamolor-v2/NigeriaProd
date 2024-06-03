start transaction;
delete from nigeria.last_recharge where tbl_dt=yyyymmdd;
insert into nigeria.last_recharge
select msisdn_key,tbl_dt as last_recharge_date,recharge_amt,tbl_dt from (
select a.tbl_dt,a.msisdn_key,a.transaction_amt recharge_amt,
row_number() over (partition by a.msisdn_key order by a.original_timestamp_enrich desc,a.transaction_amt) rnk
from flare_8.cs5_air_refill_ma a
where a.tbl_dt = yyyymmdd and a.transaction_amt>0
) where rnk=1;
commit;
