select 
MSISDN,
DONOR_ID,
RECIPIENT_ID,
DATE_TIME_STAMP
from provident.MNP_PORTING_BROADCAST  WHERE DATE_TIME_STAMP > to_date('${maxSeq}','yyyymmddhh24miss') 

