select translate(AUTHORIZE_CODE, chr(10)||chr(11)||chr(13) , ' '),
translate(CONTRACT, chr(10)||chr(11)||chr(13) , ' '),
translate(CUSTOMER_PO_NO, chr(10)||chr(11)||chr(13) , ' '),
translate(CUSTOMER_NO, chr(10)||chr(11)||chr(13) , ' '),
translate(DATE_ENTERED, chr(10)||chr(11)||chr(13) , ' '),
translate(ORDER_ID,chr(10)||chr(11)||chr(13) , ' '),
translate(ORDER_NO, chr(10)||chr(11)||chr(13) , ' '),
translate(ROWVERSION, chr(10)||chr(11)||chr(13) , ' '),
translate(CANCEL_REASON, chr(10)||chr(11)||chr(13) , ' '),
translate(ROWSTATE ,chr(10)||chr(11)||chr(13) , ' ')
from IFSAPP.CUSTOMER_ORDER_TAB



  
