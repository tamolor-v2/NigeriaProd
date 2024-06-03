echo ""  >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group1_$(date +"%Y-%m-%d").txt"
echo "\n start date \n" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group1_$(date +"%Y-%m-%d").txt"
echo $(date +"%Y-%m-%d %H:%M:%S") >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group1_$(date +"%Y-%m-%d").txt"
echo  "start CB_POS_TRANSACTIONS" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group1_$(date +"%Y-%m-%d").txt"
bash /nas/share05/tools/ExtractTools/CB_POS_TRANSACTIONS/CB_POS_TRANSACTIONS.sh
echo  "end CB_POS_TRANSACTIONS" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group1_$(date +"%Y-%m-%d").txt"
echo  "start CB_RETAIL_OUTLETS" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group1_$(date +"%Y-%m-%d").txt"
bash /nas/share05/tools/ExtractTools/CB_RETAIL_OUTLETS/CB_RETAIL_OUTLETS.sh
echo  "end CB_RETAIL_OUTLETS" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group1_$(date +"%Y-%m-%d").txt"
echo  "start BUDGET_PERIOD_AMOUNT" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group1_$(date +"%Y-%m-%d").txt"
bash /nas/share05/tools/ExtractTools/BUDGET_PERIOD_AMOUNT/BUDGET_PERIOD_AMOUNT.sh
echo  "end BUDGET_PERIOD_AMOUNT" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group1_$(date +"%Y-%m-%d").txt"
echo  "start CORPORATE_FORM" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group1_$(date +"%Y-%m-%d").txt"
bash /nas/share05/tools/ExtractTools/CORPORATE_FORM/CORPORATE_FORM.sh
echo  "end CORPORATE_FORM" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group1_$(date +"%Y-%m-%d").txt"
echo  "start CUST_ORD_CUSTOMER_ENT" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group1_$(date +"%Y-%m-%d").txt"
bash /nas/share05/tools/ExtractTools/CUST_ORD_CUSTOMER_ENT/CUST_ORD_CUSTOMER_ENT.sh
echo  "end CUST_ORD_CUSTOMER_ENT" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group1_$(date +"%Y-%m-%d").txt"
echo  "start CUSTOMER_ORDER_TYPE" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group1_$(date +"%Y-%m-%d").txt"
bash /nas/share05/tools/ExtractTools/CUSTOMER_ORDER_TYPE/CUSTOMER_ORDER_TYPE.sh
echo  "end CUSTOMER_ORDER_TYPE" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group1_$(date +"%Y-%m-%d").txt"
echo  "start CUSTOMER_CREDIT_INFO" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group1_$(date +"%Y-%m-%d").txt"
bash /nas/share05/tools/ExtractTools/CUSTOMER_CREDIT_INFO/CUSTOMER_CREDIT_INFO.sh
echo  "end CUSTOMER_CREDIT_INFO" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group1_$(date +"%Y-%m-%d").txt"
echo  "start CUSTOMER_DETAILS" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group1_$(date +"%Y-%m-%d").txt"
bash /nas/share05/tools/ExtractTools/CUSTOMER_DETAILS/CUSTOMER_DETAILS.sh
echo  "end CUSTOMER_DETAILS" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group1_$(date +"%Y-%m-%d").txt"
echo  "start CUSTOMER_GROUP" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group1_$(date +"%Y-%m-%d").txt"
bash /nas/share05/tools/ExtractTools/CUSTOMER_GROUP/CUSTOMER_GROUP.sh
echo  "end CUSTOMER_GROUP" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group1_$(date +"%Y-%m-%d").txt"
echo  "start CUSTOMER_COMM_INFO_METHOD" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group1_$(date +"%Y-%m-%d").txt"
bash /nas/share05/tools/ExtractTools/CUSTOMER_COMM_INFO_METHOD/CUSTOMER_COMM_INFO_METHOD.sh
echo  "end CUSTOMER_COMM_INFO_METHOD" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group1_$(date +"%Y-%m-%d").txt"
echo  "start CODE_G" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group1_$(date +"%Y-%m-%d").txt"
bash /nas/share05/tools/ExtractTools/CODE_G/CODE_G.sh
echo  "end CODE_G" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group1_$(date +"%Y-%m-%d").txt"
echo  "start CODE_F" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group1_$(date +"%Y-%m-%d").txt"
bash /nas/share05/tools/ExtractTools/CODE_F/CODE_F.sh
echo  "end CODE_F" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group1_$(date +"%Y-%m-%d").txt"

echo "\n end date \n" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group1_$(date +"%Y-%m-%d").txt"
echo $(date +"%Y-%m-%d %H:%M:%S") >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group1_$(date +"%Y-%m-%d").txt"
echo " finish " >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group1_$(date +"%Y-%m-%d").txt"
echo $(date +"%Y-%m-%d %H:%M:%S") >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group1_$(date +"%Y-%m-%d").txt"
