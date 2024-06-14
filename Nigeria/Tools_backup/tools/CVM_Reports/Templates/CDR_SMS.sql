start transaction;
insert into engine_room.CDR_SMS 
select 
to_hex(md5(to_utf8((coalesce (try_cast(a.msisdn_key as varchar),''))))), 
to_hex(md5(to_utf8((coalesce (try_cast(a.otherpartymsisdn_enrich as varchar),''))))) , 
a.original_timestamp_enrich ,
upper(call_type_enrich_desc),
network_type_enrich_desc,
CASE 
WHEN ((a.otherpartymnprn = '40') OR (a.otherpartymnprn = '79')) 
THEN 'MTN' ELSE null 
END , 
CASE 
WHEN (a.otherpartymnprn = '42') THEN 'glo'
WHEN (a.otherpartymnprn = '41') THEN 'Airtel' 
WHEN (a.otherpartymnprn = '49') THEN 'Etisalat' 
WHEN (a.otherpartymnprn = '79') THEN 'Visafone/MTN' 
ELSE null 
END, 
cast(a.call_duration_enrich as varchar), 
a.totalcharge_money , 
replace(a.cell_id_enrich ,'-','') , 
b.tac , 
a.kamanja_loaded_date,
A.tbl_dt
from flare_8.cs6_ccn_cdr a
left join flare_8.dmc_dump_all b on (a.msisdn_key = b.msisdn_key AND a.tbl_dt = b.tbl_dt) 
where a.tbl_dt = yyyymmddRunDate
and a.servicetypeenrich = 'SMS';
commit;
