start transaction;
delete from nigeria.vlr where tbl_dt=20190108;
insert into nigeria.vlr
select udc.msisdn_key,max(udc.vlradd) vlradd,udc.tbl_dt 
from flare_8.udc_dump udc 
where udc.tbl_dt=20190108
group by udc.tbl_dt,udc.msisdn_key;
commit;
