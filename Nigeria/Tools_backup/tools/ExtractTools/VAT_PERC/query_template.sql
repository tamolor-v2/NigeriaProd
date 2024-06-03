select
translate( VAT_CODE_ID, chr(10)||chr(11)||chr(13) , ' '), 
'"' || translate( DESCRIPTION, chr(10)||chr(11)||chr(13) , ' ') || '"', 
translate( VAT_PERCENT, chr(10)||chr(11)||chr(13) , ' ')
from IFSAPP.VAT_PERC

