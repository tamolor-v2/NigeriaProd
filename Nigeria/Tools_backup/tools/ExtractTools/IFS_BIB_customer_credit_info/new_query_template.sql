select
translate(CUSTOMER_ADDRESS, chr(10)||chr(11)||chr(13) , ' ')
,translate(EMAIL_ADDRESS, chr(10)||chr(11)||chr(13) , ' ')
,translate(CUSTOMER_MSISDN, chr(10)||chr(11)||chr(13) , ' ')
,translate(CUSTOMER_NAME, chr(10)||chr(11)||chr(13) , ' ')
,translate(CUSTOMER_CODE, chr(10)||chr(11)||chr(13) , ' ')
,CREDIT_LIMIT
,translate(BANK_NAME, chr(10)||chr(11)||chr(13) , ' ')
,BANK_GUARANTEE_AMOUNT
,translate(BG_REF_NO, chr(10)||chr(11)||chr(13) , ' ')
,ISSUE_DATE
,BG_EXPIRY_DATE
,TOTAL_CREDIT_SALES
,TOTAL_HCR_SALES
,TOTAL_CASH_SALES
,TOTAL_SALES
,to_char(TRANSACTION_DATE,'yyyymmdd hh24miss')
from IFsapp.IFS_BIB_customer_credit_info
WHERE to_char(transaction_date,'YYYYMMDD')=${DATE}
