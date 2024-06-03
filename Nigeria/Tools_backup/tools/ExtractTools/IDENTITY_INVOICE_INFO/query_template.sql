select
translate( IDENTITY, chr(10)||chr(11)||chr(13) , ' '), 
translate( OBJVERSION, chr(10)||chr(11)||chr(13) , ' '),
EXPIRE_DATE,
VAT_NO,
PAY_TERM_ID,
PARTY_TYPE,
GROUP_ID
from IFSAPP.IDENTITY_INVOICE_INFO
