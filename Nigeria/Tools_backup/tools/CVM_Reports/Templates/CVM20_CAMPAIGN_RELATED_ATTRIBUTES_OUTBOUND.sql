start transactions;
delete from cvm_db.CVM20_CAMPAIGN_RELATED_ATTRIBUTES_OUTBOUND where tbl_dt = yyyymmddRunDate;
insert into cvm_db.cvm20_campaign_related_attributes_outbound
(msisdn_key , yearid , monthid, weekid , week_started , week_ended , offer_presented_outbound , offer_presented_outbound_sms_count ,
offer_presented_outbound_obd_count , offer_presented_outbound_facebook_count, offer_presented_outbound_others_count, offer_redeemed_count_outbound, tbl_dt)
select  msisdn_key
,YearID
,monthid
,WeekID
,week_started
,week_ended
,sum(offer_presented_outbound) offer_presented_outbound
,sum(offer_presented_outbound_sms_count )   offer_presented_outbound_sms_count
,sum(offer_presented_outbound_obd_count  ) offer_presented_outbound_obd_count
,sum(offer_presented_outbound_facebook_count  )     offer_presented_outbound_facebook_count
,sum(offer_presented_outbound_others_count   ) offer_presented_outbound_others_count
,sum(offer_redeemed_count_outbound ) offer_redeemed_count_outbound
,tbl_dt from
(
select
msisdn_key ,
date_format(DATE_TRUNC('week', date_parse(CAST(date_key AS varchar(10)), '%Y%m%d')),'%Y') YearID,
date_format(DATE_TRUNC('week', date_parse(CAST(date_key AS varchar(10)), '%Y%m%d')),'%Y%m')monthid ,
date_format(DATE_TRUNC('week', date_parse(CAST(date_key AS varchar(10)), '%Y%m%d')),'%v') WeekID,
cast( date_format(DATE_TRUNC('week', date_parse(CAST(date_key AS varchar(10)), '%Y%m%d')),'%Y%m%d') as bigint) week_started,
cast(date_format(DATE_TRUNC('week', date_parse(CAST(date_key AS varchar(10)), '%Y%m%d'))+ interval '6' day,'%Y%m%d') as bigint) week_ended
, cast(delivered_status as int)  offer_presented_outbound
, case when upper(channel) in ('SMS') then cast(delivered_status as int) else 0 end offer_presented_outbound_sms_count
,case when upper(channel) in ('OBD') then cast(delivered_status as int) else 0 end offer_presented_outbound_obd_count
,case when upper(channel) in ('FACEBOOK') then cast(delivered_status as int) else 0 end offer_presented_outbound_facebook_count
,case when upper(channel) not in ('SMS','OBD','FACEBOOK') then cast(delivered_status as int) else 0 end  offer_presented_outbound_others_count
,cast(conversion_status as int)     offer_redeemed_count_outbound
, channel, offer_id,
start_date, campaign_id , bc_id, campaign_name  , broadcast_name ,
sub_sts ,acknowledge_status ,delivered_status,conversion_status ,cvm_id
,cast( date_format(DATE_TRUNC('week', date_parse(CAST(date_key AS varchar(10)), '%Y%m%d')),'%Y%m%d') as int) tbl_dt
from flare_8.Flytxt_Campaign_Events_Data
where tbl_dt between yyyymmddRunDate and yyyyRunDateWeek )
group by  msisdn_key
,YearID
,monthid
,WeekID
,week_started
,week_ended
, tbl_dt;
commit;
