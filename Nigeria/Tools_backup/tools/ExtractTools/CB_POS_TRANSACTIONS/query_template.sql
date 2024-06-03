select  translate(LOCATION_CODE_V, chr(10)||chr(11)||chr(13) , ' '), 
translate(trim(to_char(TOTAL_AMOUNT_N, '99999999999999999999999999999999999999999')), chr(10)||chr(11)||chr(13) , ' '),  
TO_DATE(TO_CHAR(TRANS_DATE_DT, 'yyyy-MM-DD HH:mm:ss'), 'yyyy-MM-DD HH:mi:ss'), 
translate(trim(to_char(AMOUNT_N, '99999999999999999999999999999999999999999')), chr(10)||chr(11)||chr(13) , ' ')  from TT_MSO_1.CB_POS_TRANSACTIONS  WHERE SUBSTR(TRANSACTION_NUM_V,-1,1)='${MSSDN}'



