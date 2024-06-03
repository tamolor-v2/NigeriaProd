 select
translate(SUBSCRIBER_NAME_V, chr(10)||chr(11)||chr(13) , ' ')
 ,translate(PAY_MODE_CODE_V, chr(10)||chr(11)||chr(13) , ' ')
 ,translate(MODE_PAYMENT_V, chr(10)||chr(11)||chr(13) , ' ')
 ,translate(TRANS_NUM_V, chr(10)||chr(11)||chr(13) , ' ')
 ,translate(CHEQUE_NUM_V, chr(10)||chr(11)||chr(13) , ' ')
 ,translate(USER_NAME_V, chr(10)||chr(11)||chr(13) , ' ')
 ,translate(LOCATION_CODE_V, chr(10)||chr(11)||chr(13) , ' ')
 ,translate(LOCATION_V, chr(10)||chr(11)||chr(13) , ' ')
 ,translate(BANK_NAME_V, chr(10)||chr(11)||chr(13) , ' ')
 ,translate(SUBS_CATEGORY_DESC_V, chr(10)||chr(11)||chr(13) , ' ')
 ,translate(SUBS_SUB_CATEGORY_DESC_V, chr(10)||chr(11)||chr(13) , ' ')
 ,translate(MSISDN_V, chr(10)||chr(11)||chr(13) , ' ')
 ,translate(SERVICE_CODE_V, chr(10)||chr(11)||chr(13) , ' ')
 ,TRANS_DATE_D
 ,CHQ_EXP_DATE_D
 ,ACCOUNT_CODE_N
 ,SUBSCRIBER_CODE_N
 ,USER_CODE_N
 ,PAYMENT_AMT_N
 ,ADV_AMT_N
 ,translate(AM_NAME_V, chr(10)||chr(11)||chr(13) , ' ')
 ,OUTSTANDING_AMT_N
 ,translate(PACKAGE_NAME_V, chr(10)||chr(11)||chr(13) , ' ')
 ,translate(DESCRIPTION_V, chr(10)||chr(11)||chr(13) , ' ')
FROM   tt_mso_1.payment_report_all_MAY19 --${DATE}

