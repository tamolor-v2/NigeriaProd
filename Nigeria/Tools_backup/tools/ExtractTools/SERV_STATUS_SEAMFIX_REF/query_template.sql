SELECT /*+ parallel(10)*/ MSISDN,
       FIRST_NAME_V,
       LAST_NAME_V,
       STATUS,
       KYC_NUMBER,
       TO_CHAR(ACTI_REG_DATE AT TIME ZONE 'Africa/Algiers', 'yyyymmddhh24miss') ACTI_REG_DATE,
       MOTHER_MAIDEN_NAME,
       TO_CHAR(BIRTHDATE AT TIME ZONE 'Africa/Algiers', 'yyyymmddhh24miss') BIRTHDATE,
       CHANNEL,
       CONTRACT_TYPE_V
  FROM TT_MSO_1.SERV_STATUS_SEAMFIX_REF_VW
 WHERE  TRUNC(ACTI_REG_DATE AT TIME ZONE 'Africa/Algiers') >= TO_DATE('${DATE}', 'yyyymmdd')
    AND TRUNC(ACTI_REG_DATE AT TIME ZONE 'Africa/Algiers') < TO_DATE('${DATE}', 'yyyymmdd') + 1
