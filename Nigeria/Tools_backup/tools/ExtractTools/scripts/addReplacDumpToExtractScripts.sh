feeds=("CB_POS_TRANSACTIONS"
"CB_RETAIL_OUTLETS"
"BUDGET_PERIOD_AMOUNT "
"CUST_ORD_CUSTOMER_ENT"
"CUSTOMER_DETAILS"
"CUSTOMER_INFO_TAB"
"CUSTOMER_ORDER_HISTORY"
"CUSTOMER_ORDER_LINE_TAB"
"CUSTOMER_ORDER_TAB"
"DBA_ROLE_PRIVS"
"DBA_TAB_PRIVS"
"FND_USER_ROLE"
"HISTORY_LOG"
"INVENTORY_TRANSACTION_HIST"
"LEDGER_ITEM_TAB"
"PAYMENT_TAB"
"PAYMENT_TERM"
"PURCH_REQ_APPROVAL"
"PURCHASE_ORDER"
"PURCHASE_ORDER_APPROVAL"
"PURCHASE_ORDER_HIST"
"PURCHASE_ORDER_LINE"
"PURCHASE_RECEIPT_TAB"
"PURCHASE_REQUISITION_TAB"
"SUPPLIER_INFO"
"BLACKLIST_HISTORY"
"DEALER_TYPE"
"ENROLLMENT_REF"
"KM_USER"
"KM_USER_ROLE"
"KYC_DEALER"
"NODE"
"NODE_ASSIGNMENT"
"SMS_ACTIVATION_REQUEST"
"STATE"
"MASTER_DATA_VIEW")

#feedss=("CB_POS_TRANSACTIONS")

for feed in "${feeds[@]}"
do
	extract_path=/nas/share05/tools/ExtractTools/$feed
	echo "sed -i \"20ibash /nas/share05/tools/ExtractTools/scripts/replaceDump.sh ${feed} ${yest} \\n\" ${extract_path}/${feed}.sh"
	#sed -i "20ibash /nas/share05/tools/ExtractTools/scripts/replaceDump.sh ${feed} \${yest} \n" ${extract_path}/${feed}.sh 
	sed -i -e "s/bash \/nas\/share05\/tools\/ExtractTools\/scripts\/replaceDump.sh ${feed}/bash \/nas\/share05\/tools\/ExtractTools\/scripts\/replaceDump.sh ${feed} \${yest}/g" ${extract_path}/${feed}.sh

done
