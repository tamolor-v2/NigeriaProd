select
translate(ACCOUNT, chr(10)||chr(11)||chr(13) , ' '),
translate(ACCOUNT_DESC, chr(10)||chr(11)||chr(13) , ' '),
translate(ACCOUNTING_YEAR, chr(10)||chr(11)||chr(13) , ' '),
translate(AMOUNT, chr(10)||chr(11)||chr(13) , ' '),
translate(CREATOR_DESC, chr(10)||chr(11)||chr(13) , ' '),
translate(CREDIT_AMOUNT,chr(10)||chr(11)||chr(13) , ' '),
translate( CURRENCY_CODE, chr(10)||chr(11)||chr(13) , ' '),
translate(DEBET_AMOUNT, chr(10)||chr(11)||chr(13) , ' '),
translate(OBJVERSION, chr(10)||chr(11)||chr(13) , ' '),
translate(ROW_NO, chr(10)||chr(11)||chr(13) , ' '),
translate(THIRD_CURRENCY_AMOUNT, chr(10)||chr(11)||chr(13) , ' '),
translate(THIRD_CURRENCY_CREDIT_AMOUNT, chr(10)||chr(11)||chr(13) , ' '),
translate(THIRD_CURRENCY_DEBIT_AMOUNT, chr(10)||chr(11)||chr(13) , ' '),
translate(TRANS_CODE, chr(10)||chr(11)||chr(13) , ' '),
translate(VOUCHER_DATE, chr(10)||chr(11)||chr(13) , ' '),
translate(VOUCHER_NO, chr(10)||chr(11)||chr(13) , ' '),
translate(VOUCHER_TYPE, chr(10)||chr(11)||chr(13) , ' ')
from IFSAPP.GEN_LED_VOUCHER_ROW
