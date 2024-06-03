start transaction;
delete from cvm_db.cvm20_voice_sms_incoming where tbl_dt= yyyymmddRunDate;
insert into cvm_db.CVM20_VOICE_SMS_INCOMING      
(msisdn_key,yearid,monthid,weekid,week_started,week_ended,
voi_onnet_in_count, voi_offnet_in_count, voi_int_in_count, voi_onnet_in_sec,voi_offnet_in_sec, voi_int_in_sec, sms_onnet_in_count, 
sms_offnet_in_count, sms_int_in_count, voi_in_count, sms_in_count, voi_in_sec, tbl_dt)
select
cast(term_num as varchar) MSISDN_KEY,
date_format(DATE_TRUNC('week', date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d')),'%Y') YearID, 
date_format(DATE_TRUNC('week', date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d')),'%Y%m') monthid , 
date_format(DATE_TRUNC('week', date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d')),'%v') WeekID, 
cast( date_format(DATE_TRUNC('week', date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d')),'%Y%m%d') as bigint) week_started,
cast(date_format(DATE_TRUNC('week', date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d'))+ interval '6' day,'%Y%m%d') as bigint) week_ended ,
sum(case when event_typ in  ('voice:in:onnet:msc' )   then 1 else 0 end)   voi_onnet_in_count,
sum(case when event_typ in  ('voice:in:offnet:msc' )   then 1 else 0 end)  voi_offnet_in_count,
sum(case when event_typ in  ('voice:in:international:msc' )   then 1 else 0 end)  voi_int_in_count,
sum(case when event_typ in  ('voice:in:onnet:msc' )   then duration else 0 end)  voi_onnet_in_sec,
sum(case when event_typ in  ('voice:in:offnet:msc' )   then duration  else 0 end)  voi_offnet_in_sec,
sum(case when event_typ in  ('voice:in:international:msc' )   then duration else 0 end)  voi_int_in_sec,
sum(case when event_typ in  ('sms:in:onnet:msc')   then 1 else 0 end)  sms_onnet_in_count,
sum(case when event_typ in  ('sms:in:offnet:msc' )   then 1 else 0 end)  sms_offnet_in_count,
sum(case when event_typ in  ('sms:in:international:msc' )   then 1 else 0 end)  sms_int_in_count ,
sum(case when event_typ in  ('voice:in:onnet:msc','voice:in:offnet:msc','voice:in:international:msc' )   then 1 else 0 end)    voi_in_count,
sum(case when event_typ in  ('sms:in:onnet:msc','sms:in:offnet:msc' ,'sms:in:international:msc' )   then 1 else 0 end)    sms_in_count,
sum(case when event_typ in  ('voice:in:onnet:msc','voice:in:offnet:msc','voice:in:international:msc' )   then duration else 0 end)    voi_in_sec,
cast( date_format(DATE_TRUNC('week', date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d')),'%Y%m%d') as int) tbl_dt
from                                                                                  
(                                                                                                                         
select                                                                                                           
tbl_dt,                                                                                                                   
case                                                                                                                      
when on_net_ind='Y' then 'voice:in:onnet:msc'                                                                             
when on_net_ind<>'Y' and substr(orig_num,1,3)='234' then 'voice:in:offnet:msc'                                  
when on_net_ind<>'Y' and substr(orig_num,1,1)='9' then 'voice:in:international:msc'                             
when on_net_ind<>'Y' and substr(orig_num,1,1)<>'9' and orig_num<>'0' then 'voice:in:international:msc'
else 'voice:in:unknown:msc'                                                                                               
end as event_typ,  try_cast(duration as bigint)    duration  ,                                                                                                  
try_cast( orig_num  as bigint) as orig_num,try_cast(term_num as bigint) term_num                      
from 
(
select tbl_dt, on_net_ind,  terminating_number orig_num,
originating_number term_num   ,duration 
from flare_8.msc_daas                                             
where  record_type='0'   
and call_type='MT'       
and  tbl_dt between yyyymmddRunDate and yyyyRunDateWeek 
and   substr(originating_number,1,3)='234' 
)                                                                                    
union all                                                                                                                 
select                                                                                       
tbl_dt,                                                                                                                   
case                                                                                                                      
when on_net_ind='Y' then 'sms:in:onnet:msc'                                                                               
when on_net_ind<>'Y' and substr(originating_number,1,3)='234' then 'sms:in:offnet:msc'                                    
when on_net_ind<>'Y' and substr(originating_number,1,1)='9' then 'sms:in:international:msc'                               
when on_net_ind<>'Y' and substr(originating_number,1,1)<>'9' and originating_number<>'0' then 'sms:in:international:msc'  
else 'sms:in:unknown:msc'                                                                                                 
end as event_typ,    try_cast(duration as bigint)    duration  ,                                                                                                       
try_cast(originating_number as bigint) as orig_num,try_cast(terminating_number as bigint) term_num                        
from flare_8.msc_daas                                                           
where  record_type='B' and call_type='SMST'                                                                                  
and substr(terminating_number,1,3)='234'  
and tbl_dt between yyyymmddRunDate and yyyyRunDateWeek 
)
group by term_num
,date_format(DATE_TRUNC('week', date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d')),'%Y')
,date_format(DATE_TRUNC('week', date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d')),'%Y%m')
,date_format(DATE_TRUNC('week', date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d')),'%v')
,cast( date_format(DATE_TRUNC('week', date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d')),'%Y%m%d') as bigint)
,cast(date_format(DATE_TRUNC('week', date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d'))+ interval '6' day,'%Y%m%d') as bigint)
,cast( date_format(DATE_TRUNC('week', date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d')),'%Y%m%d') as int);
commit;
