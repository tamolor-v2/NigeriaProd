start transaction;
delete from nigeria.cm_rbt_event_sit_f where part_key=99;
insert into nigeria.cm_rbt_event_sit_f
select
msisdn_key,
tbl_dt,
original_timestamp,
value_of_pack,
name_of_pack,
vas_event_type_identifier,
validity,
main_balance_before_event,
main_balance_after_event,
product_id,99
from flare_8.vp_cm_rbt_event where tbl_dt=yyyymmdd;
commit;
