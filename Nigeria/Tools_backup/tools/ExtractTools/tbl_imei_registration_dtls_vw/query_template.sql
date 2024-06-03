select
CID,
MSISDN,
REGISTERED_ID,
IMEI_DETAIL,
to_char(DATE_OF_REGISTRATION,'yyyymmdd hh24miss'),
AUDIT_USER,
AGENT_EMAIL_ID,
CHANNEL_NAME,
SENDERS_MSISDN,
SHORT_CODE,
SMS_KEYWORD,
MSISDN_DETAIL,
SIM_SERIAL_DETAIL,
case when CHANNEL_NAME='TPP' then REGISTERED_ID when CHANNEL_NAME in ('SMSC','IVR') then SENDERS_MSISDN end
from tpp.tbl_imei_registration_dtls_vw where
DATE_OF_REGISTRATION between 
to_date('${history_date} 00:00:00','yyyymmdd hh24:mi:ss') and 
to_date('${history_date} 23:59:59','yyyymmdd hh24:mi:ss')
