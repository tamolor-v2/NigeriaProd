select translate(BUY_UNIT_PRICE, chr(10)||chr(11)||chr(13) , ' '),  
translate(LINE_NO, chr(10)||chr(11)||chr(13) , ' '),  
translate(REQUISITION_NO, chr(10)||chr(11)||chr(13) , ' '),  
translate(VENDOR_NO,  chr(10)||chr(11)||chr(13) , ' '), 
translate(ORIGINAL_QTY, chr(10)||chr(11)||chr(13) , ' '),  
translate(OBJSTATE,  chr(10)||chr(11)||chr(13) , ' '), 
translate(OBJVERSION,  chr(10)||chr(11)||chr(13) , ' '), 
translate(PROJECT_ID, chr(10)||chr(11)||chr(13) , ' ')
from IFSAPP.PURCHASE_REQ_LINE 



