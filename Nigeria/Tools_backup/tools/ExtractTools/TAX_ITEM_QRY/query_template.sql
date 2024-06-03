select
translate(IDENTITY, chr(10)||chr(11)||chr(13) , ' '), 
translate(PARTY_TYPE, chr(10)||chr(11)||chr(13) , ' '), 
translate(PARTY_TYPE_DB, chr(10)||chr(11)||chr(13) , ' '), 
translate(INVOICE_ID, chr(10)||chr(11)||chr(13) , ' '), 
translate(ITEM_ID, chr(10)||chr(11)||chr(13) , ' '), 
translate(TAX_ID, chr(10)||chr(11)||chr(13) , ' '), 
translate(TAX_PERCENTAGE, chr(10)||chr(11)||chr(13) , ' '), 
translate(TAX_CURR_AMOUNT, chr(10)||chr(11)||chr(13) , ' '), 
translate(TAX_DOM_AMOUNT,chr(10)||chr(11)||chr(13) , ' '), 
translate( FEE_CODE, chr(10)||chr(11)||chr(13) , ' '), 
translate(BASE_CURR_AMOUNT, chr(10)||chr(11)||chr(13) , ' '), 
translate(BASE_DOM_AMOUNT,chr(10)||chr(11)||chr(13) , ' ')
from IFSAPP.TAX_ITEM_QRY
