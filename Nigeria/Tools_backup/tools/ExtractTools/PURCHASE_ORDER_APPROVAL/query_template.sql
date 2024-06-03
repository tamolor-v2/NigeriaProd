select translate(APPROVER_SIGN,chr(10)||chr(11)||chr(13) , ' '),  
translate(DATE_APPROVED,chr(10)||chr(11)||chr(13) , ' '), 
translate(ORDER_NO,chr(10)||chr(11)||chr(13) , ' ') 
from IFSAPP.PURCHASE_ORDER_APPROVAL



 
