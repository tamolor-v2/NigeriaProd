select
MSISDN,
DONOR_ID,
RECIPIENT_ID,
DATE_TIME_STAMP
from provident.MNP_PORTING_BROADCAST WHERE DATE_TIME_STAMP >= to_date('${DATE}000000','yyyymmddhh24miss')
AND DATE_TIME_STAMP <= to_date('${DATE}235959','yyyymmddhh24miss')
