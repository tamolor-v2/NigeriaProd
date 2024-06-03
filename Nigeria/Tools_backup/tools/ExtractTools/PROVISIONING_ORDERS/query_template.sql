SELECT Z.appkeyvalue MSISDN,
(SELECT K.appkeyvalue
FROM nps.soappkey K JOIN nps.SoRecord A
ON A.SOID = K.SOID
WHERE K.appkeyname in ('IMSI')
AND K.SOID = C.SOID) IMSI,
A.soId, A.sourceType, A.sourceId, A.transactionId, A.soType, to_char(A.receiptTimestamp+60/1440,'YYYYMMDDHH24MISS') RECEIPT, to_char((B.startTimestamp+60/1440),'YYYYMMDDHH24MISS') STARTS, B.soState, B.sleState, decode(C.soResult, '0', 'SUCCESS', NULL, 'MI', 'FAILED') RESULT, to_char((C.responseTimestamp+60/1440),'YYYYMMDDHH24MISS') RESPONSE, to_char((C.respondedTimestamp+60/1440),'YYYYMMDDHH24MISS') RESPONDED, C.SoResult
FROM nps.soappkey Z, nps.SoRecord A, nps.SoTransaction B, nps.SoResponse C
WHERE A.soId  = Z.soId (+)
AND A.soId = B.soId (+)
AND A.soId = C.soId (+)
and Z.appkeyname in ('MSISDN')