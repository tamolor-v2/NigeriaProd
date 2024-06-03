select
translate(COMPANY, chr(10)||chr(11)||chr(13) , ' '),
translate(IDENTITY, chr(10)||chr(11)||chr(13) , ' '),
translate(PARTY_TYPE, chr(10)||chr(11)||chr(13) , ' '),
translate(PARTY_TYPE_DB, chr(10)||chr(11)||chr(13) , ' '),
translate(INVOICE_ID, chr(10)||chr(11)||chr(13) , ' '),
translate(AUTHORIZED, chr(10)||chr(11)||chr(13) , ' '),
translate(AUTH_ID, chr(10)||chr(11)||chr(13) , ' '),
translate(SERIES_ID, chr(10)||chr(11)||chr(13) , ' '),
translate(INVOICE_NO, chr(10)||chr(11)||chr(13) , ' '),
translate(INVOICE_DATE, chr(10)||chr(11)||chr(13) , ' '),
translate(INVOICE_TYPE, chr(10)||chr(11)||chr(13) , ' '),
translate(CURRENCY, chr(10)||chr(11)||chr(13) , ' '),
translate(GROSS_AMOUNT, chr(10)||chr(11)||chr(13) , ' '),
translate(DOM_GROSS_AMOUNT, chr(10)||chr(11)||chr(13) , ' '),
translate(VOUCHER_NO_REF, chr(10)||chr(11)||chr(13) , ' '),
translate(VOUCHER_TYPE_REF, chr(10)||chr(11)||chr(13) , ' '),
translate(OBJSTATE, chr(10)||chr(11)||chr(13) , ' ')
from IFSAPP.PAYMENT_PLAN_AUTH
