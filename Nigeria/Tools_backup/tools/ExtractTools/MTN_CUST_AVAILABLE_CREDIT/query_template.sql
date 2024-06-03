select
translate(CUSTOMER_ID, chr(10)||chr(11)||chr(13) , ' '), 
translate(CUSTOMER_NAME,chr(10)||chr(11)||chr(13) , ' '), 
translate(CREDIT_LIMIT,chr(10)||chr(11)||chr(13) , ' '), 
translate(PAYMENTS, chr(10)||chr(11)||chr(13) , ' '), 
translate(COMMISIONS, chr(10)||chr(11)||chr(13) , ' '), 
translate(UNINVOICED_ORDERS, chr(10)||chr(11)||chr(13) , ' '), 
translate(ITEMS_IN_DISPUTE, chr(10)||chr(11)||chr(13) , ' '), 
translate(TOTAL_CREDIT, chr(10)||chr(11)||chr(13) , ' '), 
translate(AVAILABLE_CREDIT, chr(10)||chr(11)||chr(13) , ' ')
from IFSAPP.MTN_CUST_AVAILABLE_CREDIT
