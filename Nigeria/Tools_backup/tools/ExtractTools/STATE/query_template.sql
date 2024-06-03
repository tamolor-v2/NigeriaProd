select ID, 
translate(ID, chr(10)||chr(11)||chr(13) , ' '), 
CODE, 
translate(NAME, chr(10)||chr(11)||chr(13) , ' '), 
translate(COUNTRY_FK, chr(10)||chr(11)||chr(13) , ' '), 
translate(REGION_FK, chr(10)||chr(11)||chr(13) , ' '), 
translate(ZONE_FK, chr(10)||chr(11)||chr(13) , ' '), 
translate(STATE_ID, chr(10)||chr(11)||chr(13) , ' ') from BIOCAPTURE.STATE 



