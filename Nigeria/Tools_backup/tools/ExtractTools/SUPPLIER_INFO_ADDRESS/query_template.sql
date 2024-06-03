select
translate( ADDRESS, chr(10)||chr(11)||chr(13) , ' '), 
translate(SUPPLIER_ID, chr(10)||chr(11)||chr(13) , ' ')
from IFSAPP.SUPPLIER_INFO_ADDRESS  