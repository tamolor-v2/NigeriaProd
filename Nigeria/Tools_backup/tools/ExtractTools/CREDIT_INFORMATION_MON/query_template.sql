select '"' || TRANS_DATE || '"'
,'"' || MSISDN || '"'
,ACCOUNT_CODE_N
,CURR_OTB
,CR_LIMIT
,INVOICE_AMT
,'"' || SUBSCRIBER_CATEGORY || '"'
,'"' || ACCOUNT_NAME || '"'
,'"' || ACCOUNT_STATUS || '"'
,SERV_ACC_LINK_CODE,
case  when CONTACT_ADDRESS LIKE '%'||chr(124)||'%' then
'"' || translate(REPLACE(CONTACT_ADDRESS,'"',''''), chr(10)||chr(11)||chr(13) , ' ')  || '"'
else translate(REPLACE(CONTACT_ADDRESS,'"',''''), chr(10)||chr(11)||chr(13) , ' ') end as CONTACT_ADDRESS
,'"' || ACTIVATION_DATE || '"'
,PAYMENT_AMT from TT_MSO_1.credit_information_AUG19
