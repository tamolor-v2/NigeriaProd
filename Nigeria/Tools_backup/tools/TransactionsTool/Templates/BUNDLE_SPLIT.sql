start transaction;
insert into engine_room.BUNDLE_SPLIT 
(bundle_name,bundle_id,is_in_splitproduct,type,service_id,voice_split,sms_split,data_split,vas_split,other_split,update_datetime,tbl_dt) select cis.bundle_name,cis.bundle_id,case when length(cast(split.product_id as varchar))>0 then 1 else 0 end as is_in_splitproduct,cis.type,cis.service_id,cast(case when length(cast(split.product_id as varchar))>0 then 1-split.rate when upper(trim(cis.type))='VOICE' then 1.0 else 0.0 end as double) as voice_split,cast(case when upper(trim(cis.type))='SMS' then 100 else 0 end as double) as sms_split,cast(case when length(cast(split.product_id as varchar))>0 then split.rate when upper(trim(cis.type)) in ('DATA') then 1.0 else 0.0 end as double) as data_split,cast(case when upper(trim(cis.type))='VAS' then 1.0 else 0.0 end as double) as vas_split,cast(case when upper(trim(cis.type)) not in ('VOICE','SMS','DATA','VAS') then 1.0 else 0.0 end as double) as other_split,date_parse(date_format(now(),'%Y%m%d %H%i%s'),'%Y%m%d %H%i%s') as update_datetime , yyyymmddRunDate as tbl_dt 
from 
(select bundle_id,bundle_name,type,service_id 
from 
( select bundle_id,bundle_name,type,service_id,row_number() over (partition by bundle_id order by case when type='VAS' then 0 when type='VOICE' then 1 when type='SMS' then 2 when type='DATA' then 3 when type='COMBO' then 4 else 5 end) as rnk 
from 
( select productid as bundle_id, productname as bundle_name, case when upper(trim(category)) in ('DATA2SHARE','SOCIAL BUNDLE','DIGITAL EBU DATA BUNDLE','EXTRAVALUE DATA','DATA BUNDLE','GOODYBAG DATA','SME BUNDLE') then 'DATA' when length(upper(trim(category)))=0 then 'OTHERS' else upper(trim(category)) end as type, servcieid as service_id 
from nigeria.vp_enterprise_catalogue 
where coalesce(try_cast(trim(productid) as bigint),-1)<>-1 union select product_id as bundle_id, product_name as bundle_name, case when upper(trim(product_category)) in ('DATA2SHARE','SOCIAL BUNDLE','DIGITAL EBU DATA BUNDLE','EXTRAVALUE DATA','DATA BUNDLE','GOODYBAG DATA','SME BUNDLE') then 'DATA' when length(upper(trim(product_category)))=0 then 'OTHERS' else upper(trim(product_category)) end as type, service_id 
from flare_8.vp_hsdp_lookup_new 
where coalesce(try_cast(trim(product_id) as bigint),-1)<>-1 union select product_id as bundle_id, product_name as bundle_name, case when upper(trim(type)) in ('DATA2SHARE','SOCIAL BUNDLE','DIGITAL EBU DATA BUNDLE','EXTRAVALUE DATA','DATA BUNDLE','GOODYBAG DATA','SME BUNDLE') then 'DATA' when length(upper(trim(type)))=0 then 'OTHERS' else upper(trim(type)) end as type, '' as service_id 
from nigeria.cis_catalogue 
where tbl_dt=(select max(tbl_dt) 
from nigeria.cis_catalogue 
where tbl_dt<=yyyymmddRunDate) and length(product_id)>0 and coalesce(try_cast(trim(product_id) as bigint),-1)<>-1)) where rnk=1) cis 
left outer join 
nigeria.vp_extravalue_split_tmp_test split 
on (cis.bundle_id=split.product_id)
;commit;
