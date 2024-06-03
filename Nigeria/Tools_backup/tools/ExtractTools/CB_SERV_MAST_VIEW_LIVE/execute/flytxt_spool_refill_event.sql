select
msisdn_key as MSISDN,
original_timestamp as DATE_TIME,
Type_of_Recharge as RECHARGE_TYPE,
coalesce(try_cast(Value_of_Recharge as decimal(10,0)),0) as RECHARGE_VALUE,
case when try_cast(substr(cast(Value_of_Recharge as varchar),index(cast(Value_of_Recharge as varchar),'.')+1) as integer)>0 then try_cast(try_cast(try_cast(cast(Value_of_Recharge as varchar) as decimal(18,2)) as double) as varchar) else substr(cast(Value_of_Recharge as varchar),1,index(cast(Value_of_Recharge as varchar),'.')-1) end as BALANCE_BEFORE_RECHARGE,
case when try_cast(substr(cast(coalesce(BALANCE_AFTER_RECHARGE,0) as varchar),index(cast(coalesce(BALANCE_AFTER_RECHARGE,0) as varchar),'.')+1) as integer)>0 then try_cast(try_cast(try_cast(cast(coalesce(BALANCE_AFTER_RECHARGE,0) as varchar) as decimal(18,2)) as double) as varchar) else substr(cast(coalesce(BALANCE_AFTER_RECHARGE,0) as varchar),1,index(cast(coalesce(BALANCE_AFTER_RECHARGE,0) as varchar),'.')-1) end as BALANCE_AFTER_RECHARGE,
COUNTER
from nigeria.cm_refill_event_sit_f;