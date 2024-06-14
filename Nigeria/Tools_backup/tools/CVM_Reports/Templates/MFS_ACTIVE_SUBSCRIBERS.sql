start transaction;
insert into engine_room.MFS_ACTIVE_SUBSCRIBERS 
select to_hex(md5(to_utf8((coalesce (msisdn,''))))),tbl_dt,case when account_status ='ACTIVE' then 'true' else 'false' end,kamanja_loaded_date,yyyymmddRunDate 
from flare_8.EWP_ACCOUNT_HOLDERS_DUMP 
where tbl_dt = yyyymmddRunDate
;commit;
