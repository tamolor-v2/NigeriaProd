select
date_key ,
seq_no_n ,
msisdn_v ,
last_name_v ,
middle_name ,
first_name_v ,
mother_maiden_v ,
gender_v ,
date_of_birth ,
dob ,
nationality_v ,
pin_ref_num_v ,
status_v ,
sim_reg_type_v ,
updated_dt ,
remarks_v ,
provident_status ,
agl_status ,
vendor_code_v ,
reg_state ,
reg_lga ,
update_timestamp ,
sim_reg_device ,
device_used_by ,
registration_center_v ,
email_address ,
residential_address ,
addr_lga ,
country ,
state_of_origin_v ,
lga_of_origin_v ,
registration_city ,
resident_lga ,
state_of_resident ,
religion_v ,
reg_date ,
transaction_id ,
eyeball_status_v,
eyeball_user_n,
eyeball_on_d,
eyeball_remarks_v,
record_locked_by_n, 
record_locked_on_d,
addnl_attrb_x, 
eyeball_type_v, 
simreg_kit_num_v, 
agent_name_v, 
serv_addnl_fld_1_v, 
serv_addnl_fld_2_v, 
serv_addnl_fld_3_v,
serv_addnl_fld_4_v, 
serv_addnl_fld_5_v, 
is_posted_to_clm, 
posted_to_clm_date, 
req_received_date_from_clm, 
eyeball_details_v, 
eyeball_recurringimg_v, 
quarantine_flag_v, 
dealer_code_v, 
quarantine_reason_v,
instance_id_n,
session_token_v,
ACTION_CODE_V
from (
select
TO_NUMBER (TO_CHAR (UPDATED_DT, 'yyyymmdd')) as date_key,
SEQ_NO_N as seq_no_n,
MSISDN_V as msisdn_v,
LAST_NAME_V as last_name_v,
EXTRACTVALUE (a.ISL_REQUEST_X,'//REQUESTDETAILS/@MIDDLE_NAME_V') as middle_name,
FIRST_NAME_V as first_name_v,
MOTHER_MAIDEN_V as mother_maiden_v,
GENDER_V as gender_v,
to_char(date_of_birth,'dd-MON-yy hh:mi:ss AM') date_of_birth,
to_char(date_of_birth,'yyyymmdd') dob,
NATIONALITY_V as nationality_v,
DECODE(EXTRACTVALUE(a.ISL_REQUEST_X, '//REQUEST/@TYPE_OF_SIM_REG_V'),'A'
,NVL(EXTRACTVALUE (a.ISL_REQUEST_X, '//BIOMETRIC_DTLS/@TRANSACTION_ID'),PIN_REF_NUM_V),PIN_REF_NUM_V) as pin_ref_num_v,
STATUS_V as status_v,
SIM_REG_TYPE_V as sim_reg_type_v,
UPDATED_DT as updated_dt,
REMARKS_V as remarks_v,
PROVIDENT_STATUS as provident_status,
AGL_STATUS as agl_status,
'SEAMFIX' as vendor_code_v,
EXTRACTVALUE (a.ISL_REQUEST_X, '//ADDNL_DTLS/@REGISTRATION_STATE') as reg_state,
EXTRACTVALUE (a.ISL_REQUEST_X, '//ADDNL_DTLS/@REGISTRATION_CITY') as reg_lga,
to_char(sysdate,'yyyymmdd hh24:mi:ss') as update_timestamp,
EXTRACTVALUE (a.ISL_REQUEST_X, '//BIOMETRIC_DTLS/@SIM_REG_DEVICE_ID') as sim_reg_device,
EXTRACTVALUE (a.ISL_REQUEST_X, '//BIOMETRIC_DTLS/@DEVICE_USER_ID') as device_used_by,
EXTRACTVALUE (a.ISL_REQUEST_X, '//ADDNL_DTLS/@REGISTRATION_CENTER_V') as registration_center_v,
EXTRACTVALUE (a.ISL_REQUEST_X, '//ADDNL_DTLS/@EMAIL_V') as email_address,
EXTRACTVALUE (a.ISL_REQUEST_X, '//PERSONAL_ADDRESS/@ADDRESS1')
  ||','||EXTRACTVALUE (a.ISL_REQUEST_X, '//PERSONAL_ADDRESS/@ADDRESS2')
  ||','||EXTRACTVALUE (a.ISL_REQUEST_X, '//PERSONAL_ADDRESS/@ADDRESS3') as residential_address,
EXTRACTVALUE (a.ISL_REQUEST_X, '//PERSONAL_ADDRESS/@ADDR_LGA') as addr_lga,
EXTRACTVALUE (a.ISL_REQUEST_X, '//PERSONAL_ADDRESS/@COUNTRY') as country,
EXTRACTVALUE (a.ISL_REQUEST_X, '//ADDNL_DTLS/@STATE_OF_ORIGIN_V') as state_of_origin_v,
EXTRACTVALUE (a.ISL_REQUEST_X, '//ADDNL_DTLS/@LGA_OF_ORIGIN_V') as lga_of_origin_v,
EXTRACTVALUE (a.ISL_REQUEST_X, '//ADDNL_DTLS/@REGISTRATION_CITY') as registration_city,
EXTRACTVALUE (a.ISL_REQUEST_X, '//BIOMETRIC_DTLS/@LGA') as resident_lga,
EXTRACTVALUE (a.ISL_REQUEST_X, '//BIOMETRIC_DTLS/@STATE') as state_of_resident,
EXTRACTVALUE (a.ISL_REQUEST_X, '//ADDNL_DTLS/@RELIGION_V') as religion_v,
EXTRACTVALUE (a.ISL_REQUEST_X, '//REQUESTDETAILS/@REGISTRATION_DATE_D') as reg_date,
EXTRACTVALUE (a.ISL_REQUEST_X, '//BIOMETRIC_DTLS/@TRANSACTION_ID') as transaction_id,
EYEBALL_STATUS_V as eyeball_status_v,
EYEBALL_USER_N as eyeball_user_n,
EYEBALL_ON_D as eyeball_on_d,
EYEBALL_REMARKS_V as eyeball_remarks_v,
RECORD_LOCKED_BY_N as record_locked_by_n,
RECORD_LOCKED_ON_D as record_locked_on_d,
ADDNL_ATTRB_X as addnl_attrb_x,
EYEBALL_TYPE_V as eyeball_type_v,
SIMREG_KIT_NUM_V as simreg_kit_num_v,
AGENT_NAME_V as agent_name_v,
SERV_ADDNL_FLD_1_V as serv_addnl_fld_1_v,
SERV_ADDNL_FLD_2_V as serv_addnl_fld_2_v,
SERV_ADDNL_FLD_3_V as serv_addnl_fld_3_v,
SERV_ADDNL_FLD_4_V as serv_addnl_fld_4_v,
SERV_ADDNL_FLD_5_V as serv_addnl_fld_5_v,
IS_POSTED_TO_CLM as is_posted_to_clm,
POSTED_TO_CLM_DATE as posted_to_clm_date,
REQ_RECEIVED_DATE_FROM_CLM as req_received_date_from_clm,
EYEBALL_DETAILS_V as eyeball_details_v,
EYEBALL_RECURRINGIMG_V as eyeball_recurringimg_v,
QUARANTINE_FLAG_V as quarantine_flag_v,
DEALER_CODE_V as dealer_code_v,
QUARANTINE_REASON_V as quarantine_reason_v,
INSTANCE_ID_N as instance_id_n,
SESSION_TOKEN_V as session_token_v,
ACTION_CODE_V
from
cbs_tbl_cust.CB_NEWREG_BIOUPDT_POOL a
where TO_NUMBER (TO_CHAR (UPDATED_DT, 'yyyymmdd'))='${FilterDATE}')
