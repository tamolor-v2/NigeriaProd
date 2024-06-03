select
translate( CORPORATE_FORM, chr(10)||chr(11)||chr(13) , ' '), 
translate( CREATION_DATE, chr(10)||chr(11)||chr(13) , ' '), 
translate( NAME, chr(10)||chr(11)||chr(13) , ' '), 
translate( ASSOCIATION_NO, chr(10)||chr(11)||chr(13) , ' '), 
translate( C_STATUS, chr(10)||chr(11)||chr(13) , ' '), 
translate( SUPPLIER_ID, chr(10)||chr(11)||chr(13) , ' '), 
translate( PARTY_TYPE, chr(10)||chr(11)||chr(13) , ' '), 
translate( PARTY, chr(10)||chr(11)||chr(13) , ' '), 
translate( PARTY_TYPE_DB, chr(10)||chr(11)||chr(13) , ' ')
from IFSAPP.SUPPLIER_INFO  