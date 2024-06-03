SELECT /*+ parallel(10)*/ ACCOUNT_LINK_CODE_N, MOBL_NUM_VOICE_V, TO_CHAR(MODIFIED_DATE_D AT TIME ZONE 'Africa/Algiers','yyyymmddhh24miss')
FROM TT_MSO_1.GSM_SERVICE_MAST
 WHERE MODIFIED_DATE_D AT TIME ZONE 'Africa/Algiers' >= TO_DATE('${DATE}0000', 'yyyymmddhh24miss')
   AND MODIFIED_DATE_D AT TIME ZONE 'Africa/Algiers' <= TO_DATE('${DATE}5959', 'yyyymmddhh24miss')
