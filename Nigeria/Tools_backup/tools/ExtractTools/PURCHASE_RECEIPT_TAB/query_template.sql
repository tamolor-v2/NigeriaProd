select translate(ARRIVAL_DATE,chr(10)||chr(11)||chr(13) , ' '), 
translate(LINE_NO,chr(10)||chr(11)||chr(13) , ' '),  
translate(ORDER_NO,chr(10)||chr(11)||chr(13) , ' '),  
translate(QTY_ARRIVED,chr(10)||chr(11)||chr(13) , ' '),  
translate(RECEIPT_NO,chr(10)||chr(11)||chr(13) , ' '),  
translate(RECEIVER,chr(10)||chr(11)||chr(13) , ' '),  
translate(RELEASE_NO,chr(10)||chr(11)||chr(13) , ' '),  
translate(QTY_INSPECTED,chr(10)||chr(11)||chr(13) , ' '),  
translate(QTY_TO_INSPECT,chr(10)||chr(11)||chr(13) , ' '),  
translate(QTY_INVOICED,chr(10)||chr(11)||chr(13) , ' '),  
translate(APPROVED_DATE,chr(10)||chr(11)||chr(13) , ' '),  
translate(FINALLY_INVOICED_DATE,chr(10)||chr(11)||chr(13) , ' '), 
translate(ROWSTATE ,chr(10)||chr(11)||chr(13) , ' ') 
from IFSAPP.PURCHASE_RECEIPT_TAB



