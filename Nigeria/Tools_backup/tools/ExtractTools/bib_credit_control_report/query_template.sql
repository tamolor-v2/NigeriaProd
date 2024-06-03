select 
PROFILE_CODE  
,translate(PROFILE_NAME, chr(10)||chr(11)||chr(13) , ' ')  
,'"' || translate(PROFILE_TYPE, chr(10)||chr(11)||chr(13) , ' ')  || '"'  
,translate(INVOICE_ACCOUNT_NAME, chr(10)||chr(11)||chr(13) , ' ')  
,INVOICE_ACCOUNT_CODE  
,SERVICE_ACCOUNT_LINK_CODE  
,'"' || translate(OLD_ACCOUNT_CODE, chr(10)||chr(11)||chr(13) , ' ')  || '"'  
,'"' || translate(SERVICE_ID, chr(10)||chr(11)||chr(13) , ' ')  || '"'  
,'"' || translate(SERVICE_NAME, chr(10)||chr(11)||chr(13) , ' ')  || '"'  
,'"' || translate(SUBSCRIBER_CATEGORY, chr(10)||chr(11)||chr(13) , ' ')  || '"'  
,'"' || translate(SUBSCRIBER_SUB_CATEGORY, chr(10)||chr(11)||chr(13) , ' ')  || '"'  
,'"' || translate(PACKAGE_NAME, chr(10)||chr(11)||chr(13) , ' ')  || '"'  
,'"' || translate(BUSINESS_TYPE, chr(10)||chr(11)||chr(13) , ' ')  || '"'  
,'"' || translate(SERVICE_STATUS, chr(10)||chr(11)||chr(13) , ' ')  || '"'  
,'"' || translate(ACCOUNT_MANAGER, chr(10)||chr(11)||chr(13) , ' ')  || '"'  
,'"' || translate(LINK_DESCRIPTION, chr(10)||chr(11)||chr(13) , ' ')  || '"'  
,'"' || translate(SOFT_SUSPENDED, chr(10)||chr(11)||chr(13) , ' ')  || '"'  
,'"' || translate(STATUS_DATE, chr(10)||chr(11)||chr(13) , ' ')  || '"'  
,'"' || translate(START_DATE, chr(10)||chr(11)||chr(13) , ' ')  || '"' 
,'"' || translate(NO_CHARGE_FLAG, chr(10)||chr(11)||chr(13) , ' ')  || '"'  
,'"' || translate(PRODUCT, chr(10)||chr(11)||chr(13) , ' ')  || '"'  
,'"' || translate(SERVICE, chr(10)||chr(11)||chr(13) , ' ')  || '"'  
,'"' || translate(TAX_PLAN, chr(10)||chr(11)||chr(13) , ' ')  || '"'  
,UNBILLED_USAGE  
,BILL_CYCLE  
,CREDIT_LIMIT  
,RENTALS  
,OUTSTANDING_BALANCE  
,CURRENT_BALANCE  
,PREVIOUS_AMOUNT_DUE  
,'"' || translate(CUST_SUPPORT_MANAGER, chr(10)||chr(11)||chr(13) , ' ')  || '"'  
,NON_RECURRING_CHARGE  
,'"' || translate(CREDIT_CONTROL_FLAG, chr(10)||chr(11)||chr(13) , ' ')  || '"'  
,DUNNING_CONTROL_FLAG
from tt_mso_1.bib_credit_control_report  

