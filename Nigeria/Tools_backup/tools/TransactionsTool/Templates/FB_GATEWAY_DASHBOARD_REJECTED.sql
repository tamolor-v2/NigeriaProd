start transaction;
delete from flare_8.vw_gateway_rejected where tbl_dt >= firstDayOfpreviousMonth ; 
insert into  flare_8.vw_gateway_rejected 
select  
*,
try_cast(date_format( 
date_add('day',-1,date_parse(cast(file_date  as varchar),'%Y%m%d')) 
 ,'%Y%m%d') as integer)
as tbl_dt
from flare_8.rejecteddata 
where file_date between firstDayOfpreviousMonth and try_cast(date_format(CAST(current_date AS DATE),'%Y%m%d') as integer)
    and upper(msgtype) like '%GATEWAY_INFO%';
commit;
