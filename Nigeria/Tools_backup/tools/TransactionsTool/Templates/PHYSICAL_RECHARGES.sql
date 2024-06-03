start transaction;
insert into engine_room.PHYSICAL_RECHARGES 
select 
ng.transactionid transaction_id,
date_format(date_parse(ng.original_timestamp_enrich ,'%d-%b-%y %H:%i:%s %p'),'%Y%m%d%H%i%s' ) date,
'' sender_msisdn,
to_hex(md5(to_utf8((cast(coalesce (ng.msisdn_key,0) as varchar ))))) receiver_msisdn, 
'sell_out' transaction_type, 
cast(try_CAST(ng.value AS double)/100 as varchar) value,
cell_id, 
'' distributor_id, 
'' stock_balance, 
kamanja_loaded_date, 
ng.tbl_dt 
from flare_8.ngvs_cdr NG 
left join (select 
replace(c.cgi_enrich,'-','') cell_id,
origintransactionid, 
voucher_serial_nr, 
transactionamount, 
msisdn_key, 
tbl_dt 
from flare_8.cs6_air_cdr c
where cdr_type_main = 'RR'
and trim(voucher_serial_nr) <> '') l on (ng.tbl_dt = l.tbl_dt 
and ng.serialnumber = l.voucher_serial_nr 
and ng.msisdn_key = l.msisdn_key 
and ng.transactionid = l.origintransactionid
)
where upper(ng.state_card_state) = 'USED' 
and ng.tbl_dt = yyyymmddRunDate;
commit;
