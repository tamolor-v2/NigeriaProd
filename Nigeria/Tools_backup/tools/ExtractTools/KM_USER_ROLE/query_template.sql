select
translate( SURNAME, chr(10)||chr(11)||chr(13) , ' '), 
translate( FIRST_NAME, chr(10)||chr(11)||chr(13) , ' '), 
'"' || translate( EMAIL_ADDRESS, chr(10)||chr(11)||chr(13) , ' ') || '"', 
translate( MOBILE, chr(10)||chr(11)||chr(13) , ' '), 
translate( ROLE, chr(10)||chr(11)||chr(13) , ' '), 
ACTIVE,
translate( ACTIVE_STATUS, chr(10)||chr(11)||chr(13) , ' '), 
translate( CREATE_DATE, chr(10)||chr(11)||chr(13) , ' '), 
translate( LAST_MODIFIED, chr(10)||chr(11)||chr(13) , ' '), 
translate( LAST_SUCCESSFUL_LOGIN, chr(10)||chr(11)||chr(13) , ' '), 
DEALER_FK
from BIOCAPTURE.KM_USER_ROLE   
