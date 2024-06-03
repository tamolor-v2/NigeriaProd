select
msisdn_key as MSISDN_NSK,
SUBSCRIBER_BIRTHDAY,
replace(ACCOUNT_TYPE,',',';') as ACCOUNT_TYPE,
msisdn_key as CONSUMER_ADDRESS,
IMEI,
IMSI,
case when x_id_sales_region is null then '' when x_id_sales_region='null' then '' when x_id_sales_region='NULL' then '' 
else coalesce(replace(replace(x_id_sales_region,',',';'),'NULL',''),'') end as XID_SALES_REGION,
case when sales_region_zone is null then '' when sales_region_zone='null' then '' when sales_region_zone='NULL' then '' 
else coalesce(replace(replace(sales_region_zone,',',';'),'NULL',''),'') end as SALES_REGION,
replace(DEVICE_TYPE,',',';') as DEVICE_TYPE,
LTE,
replace(PHONE_BRAND,',',';') as PHONE_BRAND,
replace(PHONE_MODEL,',',';') as PHONE_MODEL,
replace(PREFFERED_LANGUAGE,',',';') as PREFFERED_LANGUAGE,
replace(FIRST_NAME,',',';') as FIRST_NAME,
replace(LAST_NAME,',',';') as LAST_NAME,
MOMO_PIN_STATUS,
my_mtn_app_subscription_status as MTN_APP_SUBSCR,
UPDATE_DATE,
replace(lower(EMAIL_ADDRESS),'.comthe','.com') as EMAIL_ADDRESS
from nigeria.cm_profile_week_sit_f;
