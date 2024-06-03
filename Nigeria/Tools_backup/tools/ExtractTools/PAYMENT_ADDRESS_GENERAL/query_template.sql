select
translate( IDENTITY , chr(10)||chr(11)||chr(13) , ' '), 
translate(DESCRIPTION, chr(10)||chr(11)||chr(13) , ' '), 
translate(ACCOUNT, chr(10)||chr(11)||chr(13) , ' '), 
translate(COMPANY , chr(10)||chr(11)||chr(13) , ' '), 
translate( PARTY_TYPE , chr(10)||chr(11)||chr(13) , ' '), 
translate( PARTY_TYPE_DB , chr(10)||chr(11)||chr(13) , ' '), 
translate(WAY_ID, chr(10)||chr(11)||chr(13) , ' '), 
translate(ADDRESS_ID , chr(10)||chr(11)||chr(13) , ' ')
from IFSAPP.PAYMENT_ADDRESS_GENERAL  