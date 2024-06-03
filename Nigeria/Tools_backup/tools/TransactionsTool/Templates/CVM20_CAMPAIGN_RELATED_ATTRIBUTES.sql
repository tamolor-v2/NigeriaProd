start transaction;
insert into cvm_db.cvm20_campaign_related_attributes
( 
msisdn_key
,yearid
,monthid
,weekid
,week_started
,offer_presented_count_inbound 
,offer_presented_inbound_ussd_count
,offer_presented_inbound_smartapp_count 
,offer_presented_inbound_clm_count 
,offer_presented_inbound_others_count 
,offer_redeemed_count_inbound
,offer_redeemed_inbound_ussd_count 
,offer_redeemed_inbound_smartapp_count
,offer_redeemed_inbound_clm_count 
,offer_redeemed_inbound_others_count 
,tbl_dt ) 
select msisdn_key, 
YearID, monthid,WeekID,week_started 
,sum(offer_presented_count_inbound ) offer_presented_count_inbound 
,sum(offer_presented_inbound_ussd_count ) offer_presented_inbound_ussd_count 
,sum(offer_presented_inbound_smartapp_count ) offer_presented_inbound_smartapp_count 
,sum(offer_presented_inbound_clm_count) offer_presented_inbound_clm_count
,sum(offer_presented_inbound_others_count) offer_presented_inbound_others_count
,sum(offer_redeemed_count_inbound ) offer_redeemed_count_inbound 
,sum(offer_redeemed_inbound_ussd_count) offer_redeemed_inbound_ussd_count
,sum(offer_redeemed_inbound_smartapp_count ) offer_redeemed_inbound_smartapp_count 
,sum(offer_redeemed_inbound_clm_count ) offer_redeemed_inbound_clm_count 
,sum(offer_redeemed_inbound_others_count ) offer_redeemed_inbound_others_count 
,tbl_dt
from 
( 
select
msisdn_key , 
date_format(DATE_TRUNC('week', date_parse(CAST(date_key AS varchar(10)), '%Y%m%d')),'%Y') YearID, 
date_format(DATE_TRUNC('week', date_parse(CAST(date_key AS varchar(10)), '%Y%m%d')),'%Y%m')monthid , 
date_format(DATE_TRUNC('week', date_parse(CAST(date_key AS varchar(10)), '%Y%m%d')),'%v') WeekID, 
cast( date_format(DATE_TRUNC('week', date_parse(CAST(date_key AS varchar(10)), '%Y%m%d')),'%Y%m%d') as bigint) week_started,
cast(date_format(DATE_TRUNC('week', date_parse(CAST(date_key AS varchar(10)), '%Y%m%d'))+ interval '6' day,'%Y%m%d') as bigint) week_ended 
,cast(presented as int) offer_presented_count_inbound
,case when upper(channel) in ('USSD') then cast(presented as int) else 0 end offer_presented_inbound_ussd_count 
,case when upper(channel) in ('SMARTAPP') then cast(presented as int) else 0 end offer_presented_inbound_smartapp_count
,case when upper(channel) in ('CLM') then cast(presented as int) else 0 end offer_presented_inbound_clm_count 
,case when upper(channel) not in ('USSD','SMARTAPP','CLM') then cast(presented as int)else 0 end offer_presented_inbound_others_count 
,cast(conversion as int) offer_redeemed_count_inbound 
,case when upper(channel) in ('USSD') then cast(conversion as int)else 0 end offer_redeemed_inbound_ussd_count 
,case when upper(channel) in ('SMARTAPP') then cast(conversion as int) else 0 end offer_redeemed_inbound_smartapp_count 
,case when upper(channel) in ('CLM') then cast(conversion as int) else 0 end offer_redeemed_inbound_clm_count 
,case when upper(channel) not in ('USSD','SMARTAPP','CLM') then cast(conversion as int) else 0 end offer_redeemed_inbound_others_count
,cast( date_format(DATE_TRUNC('week', date_parse(CAST(date_key AS varchar(10)), '%Y%m%d')),'%Y%m%d') as int) tbl_dt
from flare_8.flytxt_inbound_events_data 
where tbl_dt between yyyymmddRunDate and yyyyRunDateWeek 
) group by msisdn_key, YearID, monthid,WeekID,week_started,tbl_dt; 
commit;
