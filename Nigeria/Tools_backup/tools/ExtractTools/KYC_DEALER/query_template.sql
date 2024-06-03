select
PK, 
ACTIVE, 
translate(CREATE_DATE, chr(10)||chr(11)||chr(13) , ' '), 
DELETED, 
translate(LAST_MODIFIED, chr(10)||chr(11)||chr(13) , ' '), 
translate(REPLACE(ADDRESS,'"',''''), chr(10)||chr(11)||chr(13) , ' '), 
translate(CONTACT_ADDRESS, chr(10)||chr(11)||chr(13) , ' '), 
translate(DEAL_CODE, chr(10)||chr(11)||chr(13) , ' '), 
translate(EMAIL_ADDRESS, chr(10)||chr(11)||chr(13) , ' '), 
translate(MOBILE_NUMBER, chr(10)||chr(11)||chr(13) , ' '), 
translate(NAME, chr(10)||chr(11)||chr(13) , ' '), 
ZONE_FK, 
DEALER_TYPE_FK, 
ORBITA_ID, 
KM_USER_PK, 
DEALER_DIVISION_FK
from BIOCAPTURE.KYC_DEALER  
