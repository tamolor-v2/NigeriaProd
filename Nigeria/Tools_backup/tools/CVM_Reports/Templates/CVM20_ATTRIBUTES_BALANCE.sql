start transaction;
delete from cvm_db.cvm20_attributes where tbl_dt = yyyymmddRunDate and subject_area = 'Balance';
insert into cvm_db.cvm20_attributes 
select 
msisdn_key
 , attribute
 , attribute_value attribute_value
 , null attr_str
 ,yyyymmddRunDate week_started 
 ,yyyyRunDateWeek week_ended
 , yyyymmddRunDate tbl_dt
 ,'Balance' subject_area
from (select
 msisdn_key
 ,concat(lower(substr(date_format(date_parse(try_cast(tbl_dt as varchar),'%Y%m%d'),'%W'),1,3)),'_bal') attribute
 ,round(account_balance,2) attribute_value
from nigeria.daily_sdp_dump_ma ma
where tbl_dt between yyyymmddRunDate and yyyyRunDateWeek
union all
select 
 msisdn_key
 ,'bal_avg_daily' attribute
 ,round(sum(account_balance)/7,2) attribute_value
from nigeria.daily_sdp_dump_ma
where tbl_dt between yyyymmddRunDate and yyyyRunDateWeek
group by msisdn_key
union all
select 
 msisdn_key
 ,'bal_days_less_5' attribute
 ,count(*) attribute_value
from nigeria.daily_sdp_dump_ma ma
where tbl_dt between yyyymmddRunDate and yyyyRunDateWeek
and account_balance < 5
group by msisdn_key
union all
select 
 msisdn_key
,'bal_days_negative' attribute
,count(*) attribute_value
from nigeria.daily_sdp_dump_ma ma
where tbl_dt between yyyymmddRunDate and yyyyRunDateWeek
and account_balance < 0
group by msisdn_key
union all
select 
 msisdn_key
 ,'bal_times_less_5' attribute
 ,count(*) attribute_value
from nigeria.daily_sdp_dump_ma ma
where tbl_dt between yyyymmddRunDate and yyyyRunDateWeek
and account_balance < 5
group by msisdn_key
union all
select 
 msisdn_key
 ,'bal_times_negative' attribute
 ,count(*) attribute_value
from nigeria.daily_sdp_dump_ma ma
where tbl_dt between yyyymmddRunDate and yyyyRunDateWeek
and account_balance < 0
group by msisdn_key
union all
select
 msisdn_key
 ,'Opening_Balance' attribute
 , round(account_balance,2) attribute_value
from nigeria.daily_sdp_dump_ma
where tbl_dt = ydate
union all
select
 msisdn_key
 ,'Closing_Balance' attribute
 , round(account_balance,2) attribute_value
from nigeria.daily_sdp_dump_ma
where tbl_dt = yyyyRunDateWeek
);
commit;
