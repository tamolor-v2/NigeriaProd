select
msisdn_key as MSISDN_NSK,
DATE_KEY,
coalesce(DATA_DOU,0) as DATA_DOU,
coalesce(SMS_DOU,0) as SMS_DOU,
coalesce(onnet_sms_dou,0) as ONNET_SMS_CNT,
coalesce(offnet_sms_dou,0) as OFFNET_SMS_CNT,
coalesce(international_sms_dou,0) as INT_SMS_CNT,
coalesce(IDD_DOU,0) as IDD_DOU,
coalesce(IDD_ROAM_DOU,0) as IDD_ROAM_DOU,
coalesce(international_data_dou,0) as ROAM_DATA_USAGE
from nigeria.cm_usg_data_sms_sit_f;
