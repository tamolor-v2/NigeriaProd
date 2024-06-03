select
ID,
ACTIVE,
BLACK_LISTED,
COMMISSIONED,
IS_CORPORATE,
translate( DATA_LIST_TAG_NAME, chr(10)||chr(11)||chr(13) , ' '), 
translate( DEPLOYMENT_CATEGORY, chr(10)||chr(11)||chr(13) , ' '), 
translate( INSTALLATION_TIMESTAMP, chr(10)||chr(11)||chr(13) , ' '), 
translate( INSTALLED_BY, chr(10)||chr(11)||chr(13) , ' '), 
translate( LAST_INSTALLED_UPDATE, chr(10)||chr(11)||chr(13) , ' '), 
translate( LAST_SYNC_TIME, chr(10)||chr(11)||chr(13) , ' '), 
translate( LAST_UPDATED, chr(10)||chr(11)||chr(13) , ' '), 
translate( LOCATION, chr(10)||chr(11)||chr(13) , ' '), 
translate( MAC_ADDRESS, chr(10)||chr(11)||chr(13) , ' '), 
translate( MACHINE_MANUFACTURER, chr(10)||chr(11)||chr(13) , ' '), 
translate( MACHINE_MODEL, chr(10)||chr(11)||chr(13) , ' '), 
translate( MACHINE_OS, chr(10)||chr(11)||chr(13) , ' '), 
translate( MACHINE_SERIAL_NUMBER, chr(10)||chr(11)||chr(13) , ' '), 
MIGRATED,
translate( NETWORK_CARD_NAME, chr(10)||chr(11)||chr(13) , ' '), 
translate( PREVIOUS_TAG_NAME, chr(10)||chr(11)||chr(13) , ' '), 
translate( REMARK, chr(10)||chr(11)||chr(13) , ' '), 
translate( SYNC_STATUS, chr(10)||chr(11)||chr(13) , ' '), 
PURCHASE_YEAR,
DEVICE_STATUS_FK,
KIT_TYPE_FK,
STATE_FK,
NODE_MANAGER_FK,
FIELD_SUPPORT_AGENT_FK,
DISTRICT_MANAGER_FK,
NODE_ACTIVITY_STATUS_ENUM,
DEVICE_OWNER,
DEVICE_TYPE,
ENROLLMENT_REF,
translate( BLACKLIST_DATE, chr(10)||chr(11)||chr(13) , ' '), 
GEOTRACKER_APP_VERSION,
translate( GEOTRACKER_INSTALL_DATE, chr(10)||chr(11)||chr(13) , ' ')
from BIOCAPTURE.NODE  
