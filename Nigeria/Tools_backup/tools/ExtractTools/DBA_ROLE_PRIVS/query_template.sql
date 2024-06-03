select 
translate(ADMIN_OPTION, chr(10)||chr(11)||chr(13) , ' '), 
translate(DEFAULT_ROLE, chr(10)||chr(11)||chr(13) , ' '), 
translate(GRANTED_ROLE, chr(10)||chr(11)||chr(13) , ' '), 
translate(GRANTEE, chr(10)||chr(11)||chr(13) , ' ')
from SYS.DBA_ROLE_PRIVS  