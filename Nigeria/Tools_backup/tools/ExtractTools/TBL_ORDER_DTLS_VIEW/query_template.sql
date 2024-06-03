
select
translate(ORDER_NO, chr(10)||chr(11)||chr(13), ' '),
translate(ORDER_DTLS, chr(10)||chr(11)||chr(13), ' '),
translate(STATUS,chr(10)||chr(11)||chr(13), ' '),
translate(INITIATED_BY,chr(10)||chr(11)||chr(13), ' '),
translate(LAST_UDPATEDBY, chr(10)||chr(11)||chr(13), ' '),
translate(ORDERED_DATE, chr(10)||chr(11)||chr(13), ' '),
to_char(last_updated_date,'yyyymmdd') as yyyymmdd,
translate(IFS_ORDER_NO, chr(10)||chr(11)||chr(13), ' '),
translate(REJECTION_REASON, chr(10)||chr(11)||chr(13), ' '),
translate(CANCELLATION_REASON, chr(10)||chr(11)||chr(13), ' '),
translate(INVENTORY_STATUS, chr(10)||chr(11)||chr(13), ' '),
translate(DISTRIBUTOR_ID, chr(10)||chr(11)||chr(13), ' '),
translate(TOTAL_ORDERED_AMT, chr(10)||chr(11)||chr(13), ' ')
from TPP.TBL_ORDER_DTLS_VIEW 
