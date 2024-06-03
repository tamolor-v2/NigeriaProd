SELECT /*+ parallel(6)*/ IMEI,
       DATE_OF_SUBMISSION,
       CHANNEL,
       SENDERS_MSISDN,
       REGISTERED_ID,
       AGENT_MSISDN,
       AGENT_FIRST_NAME,
       AGENT_LAST_NAME
  FROM TPP.SMS_INCENTIVE_IMEI_VW
 WHERE DATE_OF_SUBMISSION BETWEEN
       TO_DATE('${history_date} 00:00:00', 'yyyymmdd hh24:mi:ss') AND
       TO_DATE('${history_date} 23:59:59', 'yyyymmdd hh24:mi:ss')
--   AND IMEI IS NOT NULL
--   AND SUBSTR(IMEI, -1, 1) = '${anum}'
