 select
CREATED_DATE
 ,translate(MSISDN, chr(10)||chr(11)||chr(13) , ' ')
 ,translate(SR_ID, chr(10)||chr(11)||chr(13) , ' ')
 ,translate(SIM_SERIAL, chr(10)||chr(11)||chr(13) , ' ')
 ,translate(STATUS, chr(10)||chr(11)||chr(13) , ' ')
 ,translate(SUB_STATUS, chr(10)||chr(11)||chr(13) , ' ')
 ,translate(USER_ID, chr(10)||chr(11)||chr(13) , ' ')
 ,translate(USER_NAME, chr(10)||chr(11)||chr(13) , ' ')
 ,translate(RETAIL_SHOP, chr(10)||chr(11)||chr(13) , ' ')
 ,translate(COMMENTS_V, chr(10)||chr(11)||chr(13) , ' ')
 ,translate(REASON, chr(10)||chr(11)||chr(13) , ' ')
 ,translate(CURRENT_LOCATION, chr(10)||chr(11)||chr(13) , ' ')
 ,translate(CONNECT_POINT, chr(10)||chr(11)||chr(13) , ' ')
 ,translate(DEFAULT_SEGMENT, chr(10)||chr(11)||chr(13) , ' ')
 ,to_char(CREATED_DATE,'yyyymmdd hh24:mi:ss') as CREATED_DATE_CONVERTED
 ,translate(RETAIL_SHOP_CODE, chr(10)||chr(11)||chr(13) , ' ')
FROM TT_MSO_1.EDW_SIM_SWAP WHERE CREATED_DATE >= to_date('${DATE} 00:00:00','yyyymmdd hh24:mi:ss') and CREATED_DATE <= to_date('${DATE} 23:59:59','yyyymmdd hh24:mi:ss')

