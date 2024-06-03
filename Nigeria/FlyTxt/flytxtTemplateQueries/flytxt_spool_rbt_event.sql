select
msisdn_key as MSISDN_NSK,
original_timestamp as DATE_TIME,
coalesce(try_cast(value_of_pack as decimal(10,0)),0) as VALUE_OF_PACK,
replace(name_of_pack,',') as NAME_OF_PACK,
vas_event_type_identifier as RBT_TYPE_ID,
VALIDITY,
case when try_cast(substr(main_balance_before_event,index(main_balance_before_event,'.')+1) as integer)>0 then try_cast(try_cast(try_cast(main_balance_before_event as decimal(18,2)) as double) as varchar) else substr(main_balance_before_event,1,index(main_balance_before_event,'.')-1) end as MAIN_BAL_BEFORE,
case when try_cast(substr(main_balance_after_event,index(main_balance_after_event,'.')+1) as integer)>0 then try_cast(try_cast(try_cast(main_balance_after_event as decimal(18,2)) as double) as varchar) else substr(main_balance_after_event,1,index(main_balance_after_event,'.')-1) end as MAIN_BAL_AFTER,
case when product_id is null then '' when product_id='null' then '' when product_id='NULL' then '' else product_id end as VAS_CD
from nigeria.cm_rbt_event_sit_f;
