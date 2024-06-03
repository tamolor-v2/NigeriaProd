select translate(ADDRESS1, chr(10)||chr(11)||chr(13) , ' '), 
translate(ADDRESS2, chr(10)||chr(11)||chr(13) , ' '), 
translate(CITY, chr(10)||chr(11)||chr(13) , ' '), 
translate(CUSTOMER_ID, chr(10)||chr(11)||chr(13) , ' '), 
translate(NAME, chr(10)||chr(11)||chr(13) , ' '), 
translate(REGION_CODE, chr(10)||chr(11)||chr(13) , ' '), 
translate(REGION_DESCRIPTION, chr(10)||chr(11)||chr(13) , ' '), 
translate(STATE, chr(10)||chr(11)||chr(13) , ' ') 
from IFSAPP.CUSTOMER_DETAILS 




