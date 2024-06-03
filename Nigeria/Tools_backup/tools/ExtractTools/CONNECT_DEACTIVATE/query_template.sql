select
translate(MSISDN, chr(10)||chr(11)||chr(13) , ' '),
translate(IMSI, chr(10)||chr(11)||chr(13) , ' '),
translate(SERIAL_NUMBER, chr(10)||chr(11)||chr(13) , ' '),
translate(KI, chr(10)||chr(11)||chr(13) , ' '),
translate(PUK, chr(10)||chr(11)||chr(13) , ' '),
translate(STATUS, chr(10)||chr(11)||chr(13) , ' '),
to_char(PROCESS_DATE,'YYYYMMDDHH24MISS'),
NVL(to_char(INSERT_DATE,'YYYYMMDDHH24MISS'),'20070101235959'),
translate(REMARKS, chr(10)||chr(11)||chr(13) , ' '),
translate(PROV_STATUS, chr(10)||chr(11)||chr(13) , ' '),
translate(BATCH_ID, chr(10)||chr(11)||chr(13) , ' '),
translate(REC_REGION, chr(10)||chr(11)||chr(13) , ' '),
translate(REUSABLE, chr(10)||chr(11)||chr(13) , ' '),
translate(SO_ID, chr(10)||chr(11)||chr(13) , ' '),
to_char(DATE_DISCONNECTED,'YYYYMMDDHH24MISS'),
translate(CRM_FTP, chr(10)||chr(11)||chr(13) , ' ')
from nps.connect_deactivate
