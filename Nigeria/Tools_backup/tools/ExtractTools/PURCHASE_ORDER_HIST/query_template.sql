select translate(USERID, chr(10)||chr(11)||chr(13) , ' '), 
translate(HIST_OBJSTATE, chr(10)||chr(11)||chr(13) , ' '), 
translate(ORDER_NO, chr(10)||chr(11)||chr(13) , ' ')
from IFSAPP.PURCHASE_ORDER_HIST



