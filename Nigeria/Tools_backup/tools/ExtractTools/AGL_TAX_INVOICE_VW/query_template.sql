SELECT
 YEAR_MONTH,   
 SUBSCRIBER_CODE_N,   
 ACCOUNT_CODE_N, 
 BILL_CYCLE_CODE_N,   
 SERVICE_ID,   
 ACCOUNT_LINK_CODE_N,   
 PACKAGE_NAME_V, 
 NAME,  
 "PREVIOUS BALANCE",    
 PAYMENT,
 "ADJUSTMENT AND DISCOUNT", 
 "SUBSCRIPTION AND VAS",  
 "NATIONAL CALL/SMS",   
 "INTERNATIONAL CALL/SMS",  
 "ROAMING CALL/SMS",    
 DATA_USAGE,   
 VAT,   
 TOTAL_CHARGE, 
 AMOUNT_DUE   
FROM TT_MSO_1.TAX_INVOICE_VW
