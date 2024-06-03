start transaction;
delete from nigeria.cm_profile_bdail_events where tbl_dt=20190108;
insert into nigeria.cm_profile_bdail_events
SELECT
msisdn_key,
Last_Sms_Recv_DT,Last_Sms_Sent_DT,LastDataUsageDT,LastVoiceUsageDT,try_cast(MoMo_Inactivity_Days as int),
case when MoMo_RGS_dola between 0 and 89 then 'Y' else 'N' end momo_rgs_status,momoLastTxDate,tbl_dt
from (
select a.msisdn_key,
max(case 
when b.event_name in ('sms_onnet_incoming_evt','sms_offnet_incoming_evt','sms_international_incoming_evt') then a.last_dt else cast(20000101 as int) end)
Last_Sms_Recv_DT,
max(case 
when b.event_name in ('sms_onnet_outgoing_evt','sms_offnet_outgoing_evt','sms_international_outgoing_evt') then a.last_dt else cast(20000101 as int) end)
Last_Sms_Sent_DT,
max(case 
when b.event_name in ('data_outgoing_evt') then a.last_dt else cast(20000101 as int) end)
LastDataUsageDT,
max(case 
when b.event_name in ('voice_onnet_outgoing_evt','voice_offnet_outgoing_evt','voice_international_outgoing_evt',
'voice_onnet_incoming_evt','voice_offnet_incoming_evt','voice_international_incoming_evt',
'voice_roaming_outgoing_evt','voice_roaming_incoming_evt','voice_mail_outgoing_evt') then a.last_dt else cast(20000101 as int) end)
LastVoiceUsageDT,
max(case 
when b.event_name in ('mobilemoney_outgoing_evt','mobilemoney_incoming_evt','mobilemoney_feeoutgoing_evt')
then 
date_diff('day',date_parse(cast(a.last_dt as varchar),'%Y%m%d'),date_parse(cast(a.tbl_dt as varchar),'%Y%m%d'))
else 0 end) MoMo_Inactivity_Days,
max(case 
when b.event_name in ('mobilemoney_feeoutgoing_evt') then 
date_diff('day',date_parse(cast(a.last_dt as varchar),'%Y%m%d'),date_parse(cast(a.tbl_dt as varchar),'%Y%m%d'))
else 0 end) MoMo_RGS_dola,
max(case 
when b.event_name in ('mobilemoney_feeoutgoing_evt','mobilemoney_outgoing_evt')
then a.last_dt else cast(20000101 as int)
end)
momoLastTxDate,a.tbl_dt
from flare_8.customereventsconsolidated a, flare_8.event_type_lookup b 
where a.tbl_dt=20190108 and a.event_type=b.event_type
group by a.tbl_dt,a.msisdn_key
);
commit;
