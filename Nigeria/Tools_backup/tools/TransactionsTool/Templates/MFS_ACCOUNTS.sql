start transaction;
insert into engine_room.MFS_ACCOUNTS 
(date_key,account_type,accountid, msisdn,kamanja_loaded_date,tbl_dt) with mfs_active_accounts as ( select tbl_dt, kamanja_loaded_date, lower(from_profile_category) account_type , sender_account_id accountid, cast(sender_msisdn as varchar) msisdn 
from engine_room.MFS_TRANSACTIONS 
where tbl_dt = yyyymmddRunDate  and from_profile_category <> '' 
union all 
select tbl_dt,kamanja_loaded_date, lower(to_profile_category) account_type , receiver_account_id accountid, cast(receiver_msisdn as varchar) msisdn from engine_room.MFS_TRANSACTIONS where tbl_dt = yyyymmddRunDate and to_profile_category <> '') select distinct date_key , account_type, accountid, msisdn, last_kamanja_loaded_date , tbl_dt 
from 
( select cast( tbl_dt as varchar) date_key ,account_type, cast(accountid as varchar) accountid , msisdn ,max(kamanja_loaded_date) last_kamanja_loaded_date , tbl_dt from mfs_active_accounts group by cast( tbl_dt as varchar) ,cast(accountid as varchar) ,account_type, msisdn, tbl_dt )
;commit;
