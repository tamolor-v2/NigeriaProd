start transaction;
delete from nigeria.cm_profile_bdail_momo_fee where tbl_dt=yyyymmdd;
insert into nigeria.cm_profile_bdail_momo_fee
select msisdn_key,sum(abs(cast(tranamount as double))) momo_total_txns_fees,tbl_dt
from flare_8.mobile_money where
tbl_dt =yyyymmdd and tbl_dt between 
cast(date_format(date_add('day',-29,date_parse(cast(yyyymmdd as varchar),'%Y%m%d')),'%Y%m%d') as int) and yyyymmdd 
and upper(narration) like '%FEE%'
group by tbl_dt,msisdn_key;
commit;
