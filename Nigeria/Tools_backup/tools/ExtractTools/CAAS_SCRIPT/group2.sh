echo ""  >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group2_$(date +"%Y-%m-%d").txt"
echo "\n start date \n" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group2_$(date +"%Y-%m-%d").txt"
echo $(date +"%Y-%m-%d %H:%M:%S") >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group2_$(date +"%Y-%m-%d").txt"
echo  "start CUSTOMER_INFO_TAB" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group2_$(date +"%Y-%m-%d").txt"
bash /nas/share05/tools/ExtractTools/CUSTOMER_INFO_TAB/CUSTOMER_INFO_TAB.sh
echo  "end CUSTOMER_INFO_TAB" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group2_$(date +"%Y-%m-%d").txt"
echo  "start CUSTOMER_ORDER_HISTORY" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group2_$(date +"%Y-%m-%d").txt"
bash /nas/share05/tools/ExtractTools/CUSTOMER_ORDER_HISTORY/CUSTOMER_ORDER_HISTORY.sh
echo  "end CUSTOMER_ORDER_HISTORY" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group2_$(date +"%Y-%m-%d").txt"
echo  "start CUSTOMER_ORDER_LINE_TAB" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group2_$(date +"%Y-%m-%d").txt"
bash /nas/share05/tools/ExtractTools/CUSTOMER_ORDER_LINE_TAB/CUSTOMER_ORDER_LINE_TAB.sh
echo  "end CUSTOMER_ORDER_LINE_TAB" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group2_$(date +"%Y-%m-%d").txt"
echo  "start CUSTOMER_ORDER_TAB" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group2_$(date +"%Y-%m-%d").txt"
bash /nas/share05/tools/ExtractTools/CUSTOMER_ORDER_TAB/CUSTOMER_ORDER_TAB.sh
echo  "end CUSTOMER_ORDER_TAB" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group2_$(date +"%Y-%m-%d").txt"
echo  "start DAY_TRANS_DETAILS" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group2_$(date +"%Y-%m-%d").txt"
bash /nas/share05/tools/ExtractTools/DAY_TRANS_DETAILS/DAY_TRANS_DETAILS.sh
echo  "end DAY_TRANS_DETAILS" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group2_$(date +"%Y-%m-%d").txt"
echo  "start DBA_ROLE_PRIVS" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group2_$(date +"%Y-%m-%d").txt"
bash /nas/share05/tools/ExtractTools/DBA_ROLE_PRIVS/DBA_ROLE_PRIVS.sh
echo  "end DBA_ROLE_PRIVS" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group2_$(date +"%Y-%m-%d").txt"
echo  "start DBA_TAB_PRIVS" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group2_$(date +"%Y-%m-%d").txt"
bash /nas/share05/tools/ExtractTools/DBA_TAB_PRIVS/DBA_TAB_PRIVS.sh
echo  "end DBA_TAB_PRIVS" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group2_$(date +"%Y-%m-%d").txt"
echo  "start FND_USER" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group2_$(date +"%Y-%m-%d").txt"
bash /nas/share05/tools/ExtractTools/FND_USER/FND_USER.sh
echo  "end FND_USER" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group2_$(date +"%Y-%m-%d").txt"
echo  "start FND_USER_ROLE" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group2_$(date +"%Y-%m-%d").txt"
bash /nas/share05/tools/ExtractTools/FND_USER_ROLE/FND_USER_ROLE.sh
echo  "end FND_USER_ROLE" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group2_$(date +"%Y-%m-%d").txt"
echo  "start MAN_SUPP_INVOICE_ITEM" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group2_$(date +"%Y-%m-%d").txt"
bash /nas/share05/tools/ExtractTools/MAN_SUPP_INVOICE_ITEM/MAN_SUPP_INVOICE_ITEM.sh
echo  "end MAN_SUPP_INVOICE_ITEM" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group2_$(date +"%Y-%m-%d").txt"
echo  "start  MAN_SUPP_INVOICE" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group2_$(date +"%Y-%m-%d").txt"
bash /nas/share05/tools/ExtractTools/MAN_SUPP_INVOICE/MAN_SUPP_INVOICE.sh
echo  "end  MAN_SUPP_INVOICE" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group2_$(date +"%Y-%m-%d").txt"
echo  "start  HISTORY_LOG_ATTRIBUTE" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group2_$(date +"%Y-%m-%d").txt"
bash /nas/share05/tools/ExtractTools/HISTORY_LOG_ATTRIBUTE/HISTORY_LOG_ATTRIBUTE.sh
echo  "end  HISTORY_LOG_ATTRIBUTE" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group2_$(date +"%Y-%m-%d").txt"

echo "\n start end \n" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group2_$(date +"%Y-%m-%d").txt"
echo $(date +"%Y-%m-%d %H:%M:%S") >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group2_$(date +"%Y-%m-%d").txt"
echo " finish " >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group2_$(date +"%Y-%m-%d").txt"
echo $(date +"%Y-%m-%d %H:%M:%S") >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group2_$(date +"%Y-%m-%d").txt"
