start transaction;
insert into engine_room.CDR_RECHARGE 
select 
to_hex(md5(to_utf8((cast(coalesce (c.msisdn_key,0) as varchar ))))), 
c.original_timestamp_enrich,
case 
when trim(voucher_serial_nr) <> '' 
then 'physical recharge' 
else 'electronic recharge' 
end,
coalesce(try_cast(c.transactionamount as varchar),''), 
trim(replace(coalesce(c.cgi_enrich,''),'-','')), 
coalesce(tac,''),
c.kamanja_loaded_date,
c.tbl_dt 
from flare_8.cs6_air_cdr c
left outer join flare_8.dmc_dump_all d on (d.msisdn_key = c.msisdn_key and d.tbl_dt = c.tbl_dt)
where c.tbl_dt = yyyymmddRunDate
and c.cdr_type_main = 'RR';
commit;
