select 
translate(MSISDN, chr(10)||chr(11)||chr(13) , ' ')
 ,translate(SC, chr(10)||chr(11)||chr(13) , ' ')
 ,translate(PRODUCT, chr(10)||chr(11)||chr(13) , ' ')
 ,translate(PACKAGE_NAME_V, chr(10)||chr(11)||chr(13) , ' ')
 ,translate(SUBS_CATEGORY_DESC_V, chr(10)||chr(11)||chr(13) , ' ')
 ,translate(TRANS_DATE, chr(10)||chr(11)||chr(13) , ' ')
 ,translate(TRANS_NUMBER, chr(10)||chr(11)||chr(13) , ' ')
 ,translate(PAYMENT_DESC_V, chr(10)||chr(11)||chr(13) , ' ')
 ,translate(PAYMENT_AMOUNT, chr(10)||chr(11)||chr(13) , ' ')
 ,translate(STATUS_CODE_V, chr(10)||chr(11)||chr(13) , ' ')
 ,translate(SERVICE_CODE_V, chr(10)||chr(11)||chr(13) , ' ')
from tt_mso_1.payment_report_all_NOV19
