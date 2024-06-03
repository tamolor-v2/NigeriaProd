select
translate(INVOICE_DATE,chr(10)||chr(11)||chr(13) , ' '),
translate( INVOICE_ID, chr(10)||chr(11)||chr(13) , ' '),
translate(PAY_TERM_ID, chr(10)||chr(11)||chr(13) , ' '),
translate(OBJSTATE, chr(10)||chr(11)||chr(13) , ' '),
translate(PO_REF_NUMBER, chr(10)||chr(11)||chr(13) , ' '),
translate(VOUCHER_NO_REF, chr(10)||chr(11)||chr(13) , ' '),
translate(VOUCHER_TYPE_REF, chr(10)||chr(11)||chr(13) , ' '),
translate(GROSS_DOM_AMOUNT, chr(10)||chr(11)||chr(13) , ' '),
translate(IDENTITY, chr(10)||chr(11)||chr(13) , ' '),
translate(PARTY_TYPE, chr(10)||chr(11)||chr(13) , ' '),
translate(STATE, chr(10)||chr(11)||chr(13) , ' '),
translate(INVOICE_NO, chr(10)||chr(11)||chr(13) , ' '),
translate(NET_DOM_AMOUNT, chr(10)||chr(11)||chr(13) , ' '),
translate(DUE_DATE, chr(10)||chr(11)||chr(13) , ' '),
translate(NCF_REFERENCE, chr(10)||chr(11)||chr(13) , ' '),
translate(GROUP_ID, chr(10)||chr(11)||chr(13) , ' ')
from IFSAPP.MAN_SUPP_INVOICE
