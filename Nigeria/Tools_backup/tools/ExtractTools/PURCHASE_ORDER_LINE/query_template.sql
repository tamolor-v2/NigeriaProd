select translate(BUY_UNIT_PRICE,chr(10)||chr(11)||chr(13) , ' '),  
translate(DATE_ENTERED,chr(10)||chr(11)||chr(13) , ' '),  
translate(ORDER_NO,chr(10)||chr(11)||chr(13) , ' '),  
translate(REQUISITION_NO,chr(10)||chr(11)||chr(13) , ' '),  
translate(INVOICING_SUPPLIER,chr(10)||chr(11)||chr(13) , ' '),  
translate(STATE,chr(10)||chr(11)||chr(13) , ' '),  
translate(CURRENCY_CODE,chr(10)||chr(11)||chr(13) , ' '),  
translate(LINE_NO, chr(10)||chr(11)||chr(13) , ' '), 
translate(BUY_QTY_DUE,chr(10)||chr(11)||chr(13) , ' '),  
translate(RELEASE_NO,chr(10)||chr(11)||chr(13) , ' '),  
translate(BUY_UNIT_MEAS ,chr(10)||chr(11)||chr(13) , ' ')
from IFSAPP.PURCHASE_ORDER_LINE



