start transaction;
insert into engine_room.CDR_DATA 
select 
to_hex(md5(to_utf8((cast(coalesce (msisdn_key,0) as varchar ))))),
coalesce(original_timestamp_enrich ,''),
coalesce(try_cast(call_duration_enrich as varchar) ,''),
null,
coalesce(totalcharge_money ,''),
coalesce(cell_id_enrich ,''),
coalesce(tac_enrich ,''),
kamanja_loaded_date,
tbl_dt
from flare_8.cs6_ccn_cdr c
where tbl_dt = yyyymmddRunDate
and servicetypeenrich = 'GPRS';
commit;
