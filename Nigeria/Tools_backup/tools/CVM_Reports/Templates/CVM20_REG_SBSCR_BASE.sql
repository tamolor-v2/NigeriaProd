start transaction;
delete from cvm_db.CVM20_REG_SBSCR_BASE ;
insert into cvm_db.cvm20_reg_sbscr_base 
select msisdn_key, channel,count(*) cnt,cast(max(tbl_dt) as int) tbl_dt
from
(
select try_cast(msisdn_key as bigint) msisdn_key ,tbl_dt, 
'NULL' status, 
'MOD' channel 
from nigeria.modtransactions a
where tbl_dt between cast(date_format((date_parse(cast(yyyymmddRunDate as varchar), '%Y%m%d')) - interval '90' day,'%Y%m%d') as int)
and cast(date_format((date_parse(cast(yyyymmddRunDate as varchar), '%Y%m%d')) - interval '1' day,'%Y%m%d') as int)
union all
select try_cast(msisdn as bigint) msisdn, tbl_dt, 
case when upper(status)= 'EXISTING' then 'Subscription' else 'Registration' end status, 
'MYMTNAPP' channel 
from nigeria.smartapp_user b
where tbl_dt between cast(date_format((date_parse(cast(yyyymmddRunDate as varchar), '%Y%m%d')) - interval '90' day,'%Y%m%d') as int)
and cast(date_format((date_parse(cast(yyyymmddRunDate as varchar), '%Y%m%d')) - interval '1' day,'%Y%m%d') as int)
union all 
select msisdn_key, date_key,
'Subscrition' status , 'MOD' channel
from nigeria.daas_daily_usage_by_msisdn
where product_type ='RECHARGES' and event_type like '%MOD%'
and date_key between cast(date_format((date_parse(cast(yyyymmddRunDate as varchar), '%Y%m%d')) - interval '90' day,'%Y%m%d') as int)
and cast(date_format((date_parse(cast(yyyymmddRunDate as varchar), '%Y%m%d')) - interval '1' day,'%Y%m%d') as int)
union all
select msisdn_key,tbl_dt, 
case when upper(event_name) = 'SIGNUP'  then 'Registration' else 'Subscription' end status , 'AYOBA' channel 
from flare_8.ayobaevents 
where tbl_dt between cast(date_format((date_parse(cast(yyyymmddRunDate as varchar), '%Y%m%d')) - interval '90' day,'%Y%m%d') as int)
and cast(date_format((date_parse(cast(yyyymmddRunDate as varchar), '%Y%m%d')) - interval '1' day,'%Y%m%d') as int)
union all 
select distinct Msisdn_key, tbl_dt,
'PAID' status,
'MUSICTIME' channel
from nigeria.hsdp_sumd
where upper(product_name)  like '%MUSIC%TIME%'
and tbl_dt between cast(date_format((date_parse(cast(yyyymmddRunDate as varchar), '%Y%m%d')) - interval '90' day,'%Y%m%d') as int)
and cast(date_format((date_parse(cast(yyyymmddRunDate as varchar), '%Y%m%d')) - interval '1' day,'%Y%m%d') as int)
)
group by msisdn_key, channel;
commit;
