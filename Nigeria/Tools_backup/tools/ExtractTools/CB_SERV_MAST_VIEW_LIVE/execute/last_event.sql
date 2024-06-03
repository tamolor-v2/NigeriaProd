start transaction;
delete from nigeria.last_event where tbl_dt=20190108;
insert into nigeria.last_event
select a.msisdn_key,max(a.last_dt) last_voice_out_call,a.tbl_dt 
from flare_8.customereventsconsolidated a, flare_8.event_type_lookup b 
where a.tbl_dt=20190108 and a.event_type=b.event_type and b.event_name in ('voice_onnet_outgoing_evt','voice_offnet_outgoing_evt','voice_international_outgoing_evt','voice_roaming_outgoing_evt') 
group by a.tbl_dt,a.msisdn_key;
commit;
