start transaction;
insert into engine_room.CDR_SUBSCRIPTION 
select 
to_hex(md5(to_utf8((cast(coalesce (v.msisdn_key,0) as varchar ))))),
v.original_timestamp_enrich,
v.product_id vascode,
cast(totalcharge as varchar) costofsession,
v.cgi ,
coalesce(tac,''),
v.kamanja_loaded_date,
v.tbl_dt 
from dataops_prod.scapv2_split v
left outer join flare_8.dmc_dump_all d on (d.tbl_dt = v.tbl_dt and d.msisdn_key = v.msisdn_key)
where v.tbl_dt = yyyymmddRunDate 
and v.hostname in ('HWSDP','CIS');
commit;
