select
translate( CODE_F, chr(10)||chr(11)||chr(13) , ' '), 
translate( DESCRIPTION, chr(10)||chr(11)||chr(13) , ' '), 
translate( VALID_FROM, chr(10)||chr(11)||chr(13) , ' '), 
translate( VALID_UNTIL, chr(10)||chr(11)||chr(13) , ' '), 
translate( SORT_VALUE, chr(10)||chr(11)||chr(13) , ' ')
from IFSAPP.CODE_F
