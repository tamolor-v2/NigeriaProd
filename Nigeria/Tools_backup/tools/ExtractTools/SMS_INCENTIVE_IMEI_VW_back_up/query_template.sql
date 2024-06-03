Select IMEI,DATE_OF_SUBMISSION, CHANNEL, SENDERS_MSISDN, REGISTERED_ID, AGENT_MSISDN, AGENT_FIRST_NAME, AGENT_LAST_NAME    From Tpp.SMS_INCENTIVE_IMEI_VW where date_of_submission between to_date("${history_date} 00:00:00",'yyyymmdd hh24:mi:ss') and
to_date("${history_date} 23:59:59",'yyyymmdd hh24:mi:ss') And IMEI is not null And substr(IMEI,-1,1) = '${anum}'
