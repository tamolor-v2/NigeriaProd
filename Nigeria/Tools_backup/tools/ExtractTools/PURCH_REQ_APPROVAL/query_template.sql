select translate(AUTHORIZE_ID,chr(10)||chr(11)||chr(13) , ' '),  
translate(OBJVERSION,chr(10)||chr(11)||chr(13) , ' '), 
translate(REQUISITION_NO,chr(10)||chr(11)||chr(13) , ' ')   
from IFSAPP.PURCH_REQ_APPROVAL



