select
translate(INVOICE_ID, chr(10)||chr(11)||chr(13) , ' '), 
translate(PARTY_TYPE, chr(10)||chr(11)||chr(13) , ' '), 
translate(INVOICE_DATE, chr(10)||chr(11)||chr(13) , ' '), 
translate(INV_DOM_AMOUNT,chr(10)||chr(11)||chr(13) , ' '), 
translate(OBJSTATE, chr(10)||chr(11)||chr(13) , ' '), 
translate(PO_REF_NUMBER, chr(10)||chr(11)||chr(13) , ' '), 
translate(INVOICE_NO, chr(10)||chr(11)||chr(13) , ' '), 
translate(IDENTITY, chr(10)||chr(11)||chr(13) , ' '), 
translate(NAME, chr(10)||chr(11)||chr(13) , ' '), 
translate(AUTH_ID, chr(10)||chr(11)||chr(13) , ' '), 
translate(FIRST_AUTH_ID, chr(10)||chr(11)||chr(13) , ' '), 
translate(VOUCHER_DATE_REF, chr(10)||chr(11)||chr(13) , ' '), 
translate(VOUCHER_NO_REF, chr(10)||chr(11)||chr(13) , ' '), 
translate(ACCOUNTING_YEAR_REF, chr(10)||chr(11)||chr(13) , ' '), 
translate(PAY_TERM_ID, chr(10)||chr(11)||chr(13) , ' '), 
translate(ADV_INV, chr(10)||chr(11)||chr(13) , ' '), 
translate(LEDGER_ITEM_VERSION, chr(10)||chr(11)||chr(13) , ' ')
from IFSAPP.INVOICE_LEDGER_ITEM_SU_QRY
