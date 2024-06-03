select
translate( COLUMN_NAME, chr(10)||chr(11)||chr(13) , ' '), 
translate( NEW_VALUE, chr(10)||chr(11)||chr(13) , ' '), 
translate( OLD_VALUE, chr(10)||chr(11)||chr(13) , ' '), 
translate( LOG_ID, chr(10)||chr(11)||chr(13) , ' '),
translate( OBJVERSION, chr(10)||chr(11)||chr(13) , ' ')
from IFSAPP.HISTORY_LOG_ATTRIBUTE
