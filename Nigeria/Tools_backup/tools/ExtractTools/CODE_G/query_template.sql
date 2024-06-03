select
translate(CODE_G, chr(10)||chr(11)||chr(13) , ' '), 
translate(DESCRIPTION, chr(10)||chr(11)||chr(13) , ' '), 
translate(VALID_FROM, chr(10)||chr(11)||chr(13) , ' '), 
translate(VALID_UNTIL, chr(10)||chr(11)||chr(13) , ' '), 
translate(ACCOUNTING_TEXT_ID, chr(10)||chr(11)||chr(13) , ' '), 
translate(TEXT, chr(10)||chr(11)||chr(13) , ' '), 
translate(CONS_COMPANY, chr(10)||chr(11)||chr(13) , ' '), 
translate(CONS_CODE_PART, chr(10)||chr(11)||chr(13) , ' '), 
translate(CONS_CODE_PART_VALUE, chr(10)||chr(11)||chr(13) , ' '), 
translate(SORT_VALUE, chr(10)||chr(11)||chr(13) , ' '), 
translate(OBJID, chr(10)||chr(11)||chr(13) , ' '), 
translate(OBJVERSION, chr(10)||chr(11)||chr(13) , ' ')
from IFSAPP.CODE_G
