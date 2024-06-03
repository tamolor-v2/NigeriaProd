start transaction;
delete from nigeria.dnd_msisdn_report where tbl_dt=20190108;
insert into nigeria.dnd_msisdn_report
select msisdn_key,max(blocking_mode),tbl_dt 
from flare_8.mvas_dnd_msisdn_report
where tbl_dt=20190108
group by tbl_dt,msisdn_key;
commit;
