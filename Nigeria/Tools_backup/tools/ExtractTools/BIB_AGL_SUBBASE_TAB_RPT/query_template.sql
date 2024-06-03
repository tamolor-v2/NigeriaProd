select
translate(MOBL_NUM_VOICE_V, chr(10)||chr(11)||chr(13) , ' '),
translate(STATUS_CODE_V, chr(10)||chr(11)||chr(13) , ' '),
translate(SUBS_TITLE_V, chr(10)||chr(11)||chr(13) , ' '),
translate(FIRST_NAME, chr(10)||chr(11)||chr(13) , ' '),
translate(LAST_NAME, chr(10)||chr(11)||chr(13) , ' '),
translate(MIDDLE_NAME, chr(10)||chr(11)||chr(13) , ' '),
translate(GENDER, chr(10)||chr(11)||chr(13) , ' '),
translate(STATE_OF_ORIGIN, chr(10)||chr(11)||chr(13) , ' '),
translate(LGA_OF_ORIGIN, chr(10)||chr(11)||chr(13) , ' '),
translate(MOTHER_MAIDEN_NAME, chr(10)||chr(11)||chr(13) , ' '),
translate(TRANSACTION_ID, chr(10)||chr(11)||chr(13) , ' '),
translate(UPLOADED_DATE, chr(10)||chr(11)||chr(13) , ' '),
translate(VENDOR_CHANNEL, chr(10)||chr(11)||chr(13) , ' '),
translate(SIM_REG_DEVICE_ID, chr(10)||chr(11)||chr(13) , ' '),
translate(DEVICE_USER_ID, chr(10)||chr(11)||chr(13) , ' '),
translate(REG_STATE, chr(10)||chr(11)||chr(13) , ' '),
translate(REG_LGA, chr(10)||chr(11)||chr(13) , ' '),
translate(RELIGION, chr(10)||chr(11)||chr(13) , ' '),
translate(REGISTRATION_STATE, chr(10)||chr(11)||chr(13) , ' '),
translate(REGISTRATION_LGA, chr(10)||chr(11)||chr(13) , ' '),
translate(to_char(BIRTHDATE,'yyyymmdd'), chr(10)||chr(11)||chr(13) , ' '),
translate(ADDRESS, chr(10)||chr(11)||chr(13) , ' '),
translate(CITY, chr(10)||chr(11)||chr(13) , ' '),
translate(CITY_DESC, chr(10)||chr(11)||chr(13) , ' '),
translate(DISTRICT, chr(10)||chr(11)||chr(13) , ' '),
translate(DISTRICT_DESC, chr(10)||chr(11)||chr(13) , ' '),
translate(COUNTRY, chr(10)||chr(11)||chr(13) , ' '),
translate(COUNTRY_DESC, chr(10)||chr(11)||chr(13) , ' '),
translate(OCCUPATION, chr(10)||chr(11)||chr(13) , ' '),
translate(ALTERNATE_NUMBER, chr(10)||chr(11)||chr(13) , ' '),
translate(EYE_BALLING_STATUS, chr(10)||chr(11)||chr(13) , ' '),
translate(NOTIFICATION_EMAIL_ID, chr(10)||chr(11)||chr(13) , ' '),
translate(to_char(REPORT_GEN_DATE,'yyyymmdd'), chr(10)||chr(11)||chr(13) , ' ')
from TT_MSO_1.BIB_AGL_SUBBASE_TAB_RPT WHERE SUBSTR(MOBL_NUM_VOICE_V,-1,1)='${MSSDN}'