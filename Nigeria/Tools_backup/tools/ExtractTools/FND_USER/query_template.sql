select translate(IDENTITY, chr(10)||chr(11)||chr(13) , ' '),translate(OBJVERSION, chr(10)||chr(11)||chr(13) , ' '),translate(DESCRIPTION, chr(10)||chr(11)||chr(13) , ' '),translate(WEB_USER, chr(10)||chr(11)||chr(13) , ' '),translate(ACTIVE, chr(10)||chr(11)||chr(13) , ' '),translate(ORACLE_USER, chr(10)||chr(11)||chr(13) , ' ')  from IFSAPP.FND_USER  
