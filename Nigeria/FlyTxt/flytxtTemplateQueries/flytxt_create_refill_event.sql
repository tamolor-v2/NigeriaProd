start transaction;
delete from nigeria.cm_refill_event_sit_f where part_key=99;
insert into nigeria.cm_refill_event_sit_f
select
msisdn_key,
tbl_dt,
original_timestamp,
type_of_recharge,
value_of_recharge,
balance_before_recharge,
balance_after_recharge,
counter,99
from flare_8.vp_cm_refill_event where tbl_dt=yyyymmdd;
commit;
