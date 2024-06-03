select translate(DATE_ENTERED, chr(10)||chr(11)||chr(13) , ' '),  
'"' || translate(HISTORY_NO, chr(10)||chr(11)||chr(13) , ' ') || '"', 
translate(MESSAGE_TEXT, chr(10)||chr(11)||chr(13) , ' '), 
'"' || translate(ORDER_NO, chr(10)||chr(11)||chr(13) , ' ') || '"', 
'"' || translate(OBJSTATE, chr(10)||chr(11)||chr(13) , ' ') || '"',
'"' || translate(OBJVERSION, chr(10)||chr(11)||chr(13) , ' ') || '"',  
'"' || USERID || '"'
from IFSAPP.CUSTOMER_ORDER_HISTORY




