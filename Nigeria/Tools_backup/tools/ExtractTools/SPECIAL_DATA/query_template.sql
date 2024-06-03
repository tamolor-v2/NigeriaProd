
select ID,
translate(BIOMETRICDATATYPE , chr(10)||chr(11)||chr(13) , ' '),
translate(REASON , chr(10)||chr(11)||chr(13) , ' '),
BASIC_DATA_FK from BIOCAPTURE.SPECIAL_DATA  --${DATE}
