select
translate(IDENTITY, chr(10)||chr(11)||chr(13) , ' '), 
translate(PARTY_TYPE, chr(10)||chr(11)||chr(13) , ' '), 
translate(PARTY_TYPE_DB,chr(10)||chr(11)||chr(13) , ' '), 
translate(INVOICE_ID,chr(10)||chr(11)||chr(13) , ' '), 
translate( ITEM_ID, chr(10)||chr(11)||chr(13) , ' '), 
translate(REFERENCE, chr(10)||chr(11)||chr(13) , ' '), 
translate(VAT_CODE,chr(10)||chr(11)||chr(13) , ' '), 
translate(GROSS_AMOUNT, chr(10)||chr(11)||chr(13) , ' '), 
translate(GROSS_DOM_AMOUNT, chr(10)||chr(11)||chr(13) , ' '), 
translate(NET_CURR_AMOUNT, chr(10)||chr(11)||chr(13) , ' '), 
translate(OBJSTATE, chr(10)||chr(11)||chr(13) , ' ')
from IFSAPP.MAN_SUPP_INVOICE_ITEM
