start transaction;
insert into cvm_db.CVM20_REFILL_AND_SUBSCRIPTION_BACTH2 
(msisdn_key
,yearid
,monthid 
,weekid
,week_started  
,week_ended
,mon_rchg_amt  
,tue_rchg_amt  
,wed_rchg_amt  
,thu_rchg_amt  
,fri_rchg_amt  
,sat_rchg_amt  
,sun_rchg_amt  
,mon_rchg_cnt  
,tue_rchg_cnt  
,wed_rchg_cnt  
,thu_rchg_cnt  
,fri_rchg_cnt  
,sat_rchg_cnt  
,sun_rchg_cnt  
,rch_count_digital 
,rch_digital_amt 
,rch_count_voucher 
,rch_voucher_amt 
,rch_count_electronic  
,rch_electronic_amt
,rch_count_others  
,rch_others_amt
,total_rchg_count  
,tot_rchg_amt
,max_rch_deno
,tbl_dt)
select msisdn_key  ,
YearID,monthid,WeekID,week_started,week_ended
,sum(mon_rchg_amt)mon_rchg_amt 
,sum(tue_rchg_amt) tue_rchg_amt
,sum(wed_rchg_amt) wed_rchg_amt
,sum(thu_rchg_amt) thu_rchg_amt
,sum(fri_rchg_amt) fri_rchg_amt
,sum(sat_rchg_amt) sat_rchg_amt
,sum(sun_rchg_amt) sun_rchg_amt
,sum(mon_rchg_cnt) mon_rchg_cnt
,sum(tue_rchg_cnt) tue_rchg_cnt
,sum(wed_rchg_cnt) wed_rchg_cnt
,sum(thu_rchg_cnt) thu_rchg_cnt
,sum(fri_rchg_cnt) fri_rchg_cnt
,sum(sat_rchg_cnt) sat_rchg_cnt
,sum(sun_rchg_cnt) sun_rchg_cnt
,sum(rch_count_digital) rch_count_digital 
,sum(rch_digital_amt) rch_digital_amt 
,sum(rch_count_voucher) rch_count_voucher 
,sum(rch_voucher_amt) rch_voucher_amt 
,sum(rch_count_Electronic) rch_count_Electronic
,sum(rch_Electronic_amt) rch_Electronic_amt 
,sum(rch_count_others) rch_count_others
,sum(rch_others_amt) rch_others_amt 
,sum(total_rchg_count) total_rchg_count
,sum(tot_rchg_amt) tot_rchg_amt  
,max(tot_rchg_amt)  max_rch_deno
,tbl_dt 
from (
select 
msisdn_key , 
date_format(DATE_TRUNC('week', date_parse(CAST(date_key AS varchar(10)), '%Y%m%d')),'%Y') YearID, 
date_format(DATE_TRUNC('week', date_parse(CAST(date_key AS varchar(10)), '%Y%m%d')),'%Y%m') monthid ,
date_format(DATE_TRUNC('week', date_parse(CAST(date_key AS varchar(10)), '%Y%m%d')),'%v') WeekID,  
cast(  date_format(DATE_TRUNC('week', date_parse(CAST(date_key AS varchar(10)), '%Y%m%d')),'%Y%m%d') as bigint) week_started,  
cast(date_format(DATE_TRUNC('week', date_parse(CAST(date_key AS varchar(10)), '%Y%m%d'))+ interval '6' day,'%Y%m%d') as bigint)  week_ended 
,case when upper(format_datetime(date_parse(CAST(date_key AS varchar(10)), '%Y%m%d'),'E')) in ('MON') then amount else 0 end mon_rchg_amt
,case when upper(format_datetime(date_parse(CAST(date_key AS varchar(10)), '%Y%m%d'),'E')) in ('TUE') then amount else 0 end tue_rchg_amt
,case when upper(format_datetime(date_parse(CAST(date_key AS varchar(10)), '%Y%m%d'),'E')) in ('WED') then amount else 0 end wed_rchg_amt
,case when upper(format_datetime(date_parse(CAST(date_key AS varchar(10)), '%Y%m%d'),'E')) in ('THU') then amount else 0 end thu_rchg_amt
,case when upper(format_datetime(date_parse(CAST(date_key AS varchar(10)), '%Y%m%d'),'E')) in ('FRI') then amount else 0 end fri_rchg_amt
,case when upper(format_datetime(date_parse(CAST(date_key AS varchar(10)), '%Y%m%d'),'E')) in ('SAT') then amount else 0 end sat_rchg_amt
,case when upper(format_datetime(date_parse(CAST(date_key AS varchar(10)), '%Y%m%d'),'E')) in ('SUN') then amount else 0 end sun_rchg_amt
,case when upper(format_datetime(date_parse(CAST(date_key AS varchar(10)), '%Y%m%d'),'E')) in ('MON') then rec_count else 0 end mon_rchg_cnt 
,case when upper(format_datetime(date_parse(CAST(date_key AS varchar(10)), '%Y%m%d'),'E')) in ('TUE') then rec_count else 0 end tue_rchg_cnt 
,case when upper(format_datetime(date_parse(CAST(date_key AS varchar(10)), '%Y%m%d'),'E')) in ('WED') then rec_count else 0 end wed_rchg_cnt 
,case when upper(format_datetime(date_parse(CAST(date_key AS varchar(10)), '%Y%m%d'),'E')) in ('THU') then rec_count else 0 end thu_rchg_cnt 
,case when upper(format_datetime(date_parse(CAST(date_key AS varchar(10)), '%Y%m%d'),'E')) in ('FRI') then rec_count else 0 end fri_rchg_cnt 
,case when upper(format_datetime(date_parse(CAST(date_key AS varchar(10)), '%Y%m%d'),'E')) in ('SAT') then rec_count else 0 end sat_rchg_cnt 
,case when upper(format_datetime(date_parse(CAST(date_key AS varchar(10)), '%Y%m%d'),'E')) in ('SUN') then rec_count else 0 end sun_rchg_cnt 
,case when (event_type) in
('APPRECHG','BROADBAND DATA RESET','DATARESET','DEMAND','SMARTAPP','SP DATA RESET','SP ECW&DYA','SP SMART APP',
'MOD DATA','MOD FASTLINK','MOD GOODYBAG','MOD HELLOWORLDROAMING','MOD ICB',
'MOD VOICE','SMART WEB AIRTIME'  ,'MOD FASTLINKHOTDEALS')
then rec_count else 0 end 
rch_count_digital 
, case when (event_type) in
('APPRECHG','BROADBAND DATA RESET','DATARESET','DEMAND','SMARTAPP','SP DATA RESET','SP ECW&DYA','SP SMART APP',
'MOD DATA','MOD FASTLINK','MOD GOODYBAG','MOD HELLOWORLDROAMING','MOD ICB',
'MOD VOICE','SMART WEB AIRTIME'  ,'MOD FASTLINKHOTDEALS')
then amount else 0 end rch_digital_amt 
, case when (event_type) in  ('OTHER') then rec_count else 0 end  rch_count_others  
, case when (event_type) in  ('OTHER') then amount else 0 end rch_Others_amt
,case when (event_type) in ('VOUCHER') then amount else 0 end rch_voucher_amt 
,case when (event_type) in ('VOUCHER') then rec_count else 0 end rch_count_voucher 
,case when (event_type) not in ('VOUCHER') then amount else 0 end rch_electronic_amt 
,case when (event_type) not in ('VOUCHER') then rec_count else 0 end  rch_count_electronic 
,rec_count total_rchg_count 
,amount tot_rchg_amt
,cast(  date_format(DATE_TRUNC('week', date_parse(CAST(date_key AS varchar(10)), '%Y%m%d')),'%Y%m%d') as int) tbl_dt
from nigeria.daas_daily_usage_by_msisdn  
where date_key between yyyymmddRunDate and yyyyRunDateWeek 
and product_type in ('RECHARGES','VTU Other','Bank On-Demand') 
and UPPER(event_type) not like '%MIGRATION%' ) 
group by YearID,monthid,WeekID,week_started,week_ended,msisdn_key, tbl_dt;
commit;
