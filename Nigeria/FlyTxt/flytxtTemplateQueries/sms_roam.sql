start transaction;
delete from nigeria.sms_roam where tbl_dt=yyyymmdd;
insert into nigeria.sms_roam 
select msisdn_key,cast(count(*) as int) as sms_roaming_out_cnt,tbl_dt
from flare_8.vp_cs5_ccn_sms_ma_dasplit where tbl_dt=yyyymmdd
and call_type_enrich=1 and network_type_enrich_2=2
group by tbl_dt,msisdn_key;
commit;
