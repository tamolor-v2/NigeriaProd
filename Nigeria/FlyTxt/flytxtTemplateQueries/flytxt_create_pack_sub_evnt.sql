start transaction;
delete from nigeria.cm_pack_sub_evnt_sit_f where part_key=99;
insert into nigeria.cm_pack_sub_evnt_sit_f
select msisdn_key,tbl_dt,original_timestamp,cast(value_of_pack as varchar),
name_of_pack,vas_event_type_identifier,validity,main_balance_before_event,main_balance_after_event,product_id,99
from flare_8.vp_cm_pack_sub_evnt where tbl_dt=yyyymmdd;
commit;
