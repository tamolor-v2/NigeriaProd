echo ""  >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group4_$(date +"%Y-%m-%d").txt"
echo "\n start date \n" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group4_$(date +"%Y-%m-%d").txt"
echo $(date +"%Y-%m-%d %H:%M:%S") >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group4_$(date +"%Y-%m-%d").txt"
echo  "start PURCHASE_ORDER" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group4_$(date +"%Y-%m-%d").txt"
bash /nas/share05/tools/ExtractTools/PURCHASE_ORDER/PURCHASE_ORDER.sh
echo  "end PURCHASE_ORDER" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group4_$(date +"%Y-%m-%d").txt"
echo  "start PURCHASE_ORDER_APPROVAL" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group4_$(date +"%Y-%m-%d").txt"
bash /nas/share05/tools/ExtractTools/PURCHASE_ORDER_APPROVAL/PURCHASE_ORDER_APPROVAL.sh
echo  "end PURCHASE_ORDER_APPROVAL" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group4_$(date +"%Y-%m-%d").txt"
echo  "start PURCHASE_ORDER_HIST" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group4_$(date +"%Y-%m-%d").txt"
bash /nas/share05/tools/ExtractTools/PURCHASE_ORDER_HIST/PURCHASE_ORDER_HIST.sh
echo  "end PURCHASE_ORDER_HIST" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group4_$(date +"%Y-%m-%d").txt"
echo  "start PURCHASE_ORDER_LINE" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group4_$(date +"%Y-%m-%d").txt"
bash /nas/share05/tools/ExtractTools/PURCHASE_ORDER_LINE/PURCHASE_ORDER_LINE.sh
echo  "end PURCHASE_ORDER_LINE" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group4_$(date +"%Y-%m-%d").txt"
echo  "start PURCHASE_RECEIPT_TAB" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group4_$(date +"%Y-%m-%d").txt"
bash /nas/share05/tools/ExtractTools/PURCHASE_RECEIPT_TAB/PURCHASE_RECEIPT_TAB.sh
echo  "end PURCHASE_RECEIPT_TAB" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group4_$(date +"%Y-%m-%d").txt"
echo  "start PURCHASE_REQ_LINE" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group4_$(date +"%Y-%m-%d").txt"
bash /nas/share05/tools/ExtractTools/PURCHASE_REQ_LINE/PURCHASE_REQ_LINE.sh
echo  "end PURCHASE_REQ_LINE" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group4_$(date +"%Y-%m-%d").txt"
echo  "start PURCHASE_REQUISiTION_TAB" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group4_$(date +"%Y-%m-%d").txt"
bash /nas/share05/tools/ExtractTools/PURCHASE_REQUISiTION_TAB/PURCHASE_REQUISiTION_TAB.sh
echo  "end PURCHASE_REQUISiTION_TAB" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group4_$(date +"%Y-%m-%d").txt"
echo  "start RETURN_MATERIAL_LINE" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group4_$(date +"%Y-%m-%d").txt"
bash /nas/share05/tools/ExtractTools/RETURN_MATERIAL_LINE/RETURN_MATERIAL_LINE.sh
echo  "end RETURN_MATERIAL_LINE" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group4_$(date +"%Y-%m-%d").txt"
echo  "start SALES_PRICE_LIST_PART" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group4_$(date +"%Y-%m-%d").txt"
bash /nas/share05/tools/ExtractTools/SALES_PRICE_LIST_PART/SALES_PRICE_LIST_PART.sh
echo  "end SALES_PRICE_LIST_PART" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group4_$(date +"%Y-%m-%d").txt"
echo  "start SUPPLIER_INFO" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group4_$(date +"%Y-%m-%d").txt"
bash /nas/share05/tools/ExtractTools/SUPPLIER_INFO/SUPPLIER_INFO.sh
echo  "end SUPPLIER_INFO" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group4_$(date +"%Y-%m-%d").txt"
echo  "start PAYMENT_PLAN_AUTH" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group4_$(date +"%Y-%m-%d").txt"
bash /nas/share05/tools/ExtractTools/PAYMENT_PLAN_AUTH/PAYMENT_PLAN_AUTH.sh
echo  "end PAYMENT_PLAN_AUTH" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group4_$(date +"%Y-%m-%d").txt"
echo  "start MTN_CUST_AVAILABLE_CREDIT" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group4_$(date +"%Y-%m-%d").txt"
bash /nas/share05/tools/ExtractTools/MTN_CUST_AVAILABLE_CREDIT/MTN_CUST_AVAILABLE_CREDIT.sh
echo  "end MTN_CUST_AVAILABLE_CREDIT" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group4_$(date +"%Y-%m-%d").txt"

echo "\n start end \n" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group4_$(date +"%Y-%m-%d").txt"
echo $(date +"%Y-%m-%d %H:%M:%S") >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group4_$(date +"%Y-%m-%d").txt"
echo " finish " >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group4_$(date +"%Y-%m-%d").txt"
echo $(date +"%Y-%m-%d %H:%M:%S") >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group4_$(date +"%Y-%m-%d").txt"
