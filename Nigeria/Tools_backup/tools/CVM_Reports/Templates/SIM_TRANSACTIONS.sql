start transaction;
insert into engine_room.SIM_TRANSACTIONS 
(transaction_id ,date_key,sender_msisdn ,transaction_type ,receiver_msisdn, cell_id , distributor_id , stock_balance , kamanja_loaded_date , tbl_dt) select cast(a.tbl_dt as varchar) || to_hex(sha1(to_utf8(coalesce(cast(A.msisdn_key as varchar),'')))) TRANSACTION_ID ,cast(A.tbl_dt AS varchar) date_key ,'' sender_msisdn ,'sellout' transaction_type ,to_hex(sha1(to_utf8(coalesce(cast(A.msisdn_key as varchar),'')))) receiver_msisdn ,lo.bts_mu_site_id cell_id ,'' distributor_id ,'' stock_balance , cast(cast(to_unixtime(date_parse(cast(A.tbl_dt as varchar),'%Y%m%d')) as bigint) as varchar) kamanja_loaded_date ,A.tbl_dt tbl_dt 
from  nigeria.daily_activation_base a 
left join 
( select msisdn_key msisdn_key ,bts_mu_site_id
from nigeria.geography 
where tbl_dt = yyyymmddRunDate and aggr ='daily' AND msisdn_key <> 0) lo on A.msisdn_key = lo.msisdn_key where a.tbl_dt = yyyymmddRunDate
;commit;
