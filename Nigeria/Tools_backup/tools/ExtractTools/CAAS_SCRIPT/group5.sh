echo ""  >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group5_$(date +"%Y-%m-%d").txt"
echo "\n start date \n" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group5_$(date +"%Y-%m-%d").txt"
echo $(date +"%Y-%m-%d %H:%M:%S") >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group5_$(date +"%Y-%m-%d").txt"
echo  "start SUPPLIER_INFO_ADDRESS" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group5_$(date +"%Y-%m-%d").txt"
bash /nas/share05/tools/ExtractTools/SUPPLIER_INFO_ADDRESS/SUPPLIER_INFO_ADDRESS.sh
echo  "end SUPPLIER_INFO_ADDRESS" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group5_$(date +"%Y-%m-%d").txt"
echo  "start BLACKLIST_HISTORY" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group5_$(date +"%Y-%m-%d").txt"
bash /nas/share05/tools/ExtractTools/BLACKLIST_HISTORY/BLACKLIST_HISTORY.sh
echo  "end BLACKLIST_HISTORY" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group5_$(date +"%Y-%m-%d").txt"
echo  "start DEALER_TYPE" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group5_$(date +"%Y-%m-%d").txt"
bash /nas/share05/tools/ExtractTools/DEALER_TYPE/DEALER_TYPE.sh
echo  "end DEALER_TYPE" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group5_$(date +"%Y-%m-%d").txt"
echo  "start ENROLLMENT_REF" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group5_$(date +"%Y-%m-%d").txt"
bash /nas/share05/tools/ExtractTools/ENROLLMENT_REF/ENROLLMENT_REF.sh
echo  "end ENROLLMENT_REF" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group5_$(date +"%Y-%m-%d").txt"
echo  "start KM_USER" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group5_$(date +"%Y-%m-%d").txt"
bash /nas/share05/tools/ExtractTools/KM_USER/KM_USER.sh
echo  "end KM_USER" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group5_$(date +"%Y-%m-%d").txt"
echo  "start KM_USER_ROLE" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group5_$(date +"%Y-%m-%d").txt"
bash /nas/share05/tools/ExtractTools/KM_USER_ROLE/KM_USER_ROLE.sh
echo  "end KM_USER_ROLE" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group5_$(date +"%Y-%m-%d").txt"
echo  "start KYC_DEALER" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group5_$(date +"%Y-%m-%d").txt"
bash /nas/share05/tools/ExtractTools/KYC_DEALER/KYC_DEALER.sh
echo  "end KYC_DEALER" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group5_$(date +"%Y-%m-%d").txt"
echo  "start NODE" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group5_$(date +"%Y-%m-%d").txt"
bash /nas/share05/tools/ExtractTools/NODE/NODE.sh
echo  "end NODE" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group5_$(date +"%Y-%m-%d").txt"
echo  "start NODE_ASSIGNMENT" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group5_$(date +"%Y-%m-%d").txt"
bash /nas/share05/tools/ExtractTools/NODE_ASSIGNMENT/NODE_ASSIGNMENT.sh
echo  "end NODE_ASSIGNMENT" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group5_$(date +"%Y-%m-%d").txt"
echo  "start SMS_ACTIVATION_REQUEST" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group5_$(date +"%Y-%m-%d").txt"
bash /nas/share05/tools/ExtractTools/SMS_ACTIVATION_REQUEST/SMS_ACTIVATION_REQUEST.sh
echo  "end SMS_ACTIVATION_REQUEST" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group5_$(date +"%Y-%m-%d").txt"
echo  "start STATE" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group5_$(date +"%Y-%m-%d").txt"
bash /nas/share05/tools/ExtractTools/STATE/STATE.sh
echo  "end STATE" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group5_$(date +"%Y-%m-%d").txt"
echo  "start MASTER_DATA_VIEW" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group5_$(date +"%Y-%m-%d").txt"
bash /nas/share05/tools/ExtractTools/MASTER_DATA_VIEW/MASTER_DATA_VIEW.sh
echo  "end MASTER_DATA_VIEW" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group5_$(date +"%Y-%m-%d").txt"
echo  "start TAX_ITEM_QRY" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group5_$(date +"%Y-%m-%d").txt"
bash /nas/share05/tools/ExtractTools/TAX_ITEM_QRY/TAX_ITEM_QRY.sh
echo  "end TAX_ITEM_QRY" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group5_$(date +"%Y-%m-%d").txt"
echo  "start VAT_PERC" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group5_$(date +"%Y-%m-%d").txt"
bash /nas/share05/tools/ExtractTools/VAT_PERC/VAT_PERC.sh
echo  "end VAT_PERC" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group5_$(date +"%Y-%m-%d").txt"

echo "\n start end \n" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group5_$(date +"%Y-%m-%d").txt"
echo $(date +"%Y-%m-%d %H:%M:%S") >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group5_$(date +"%Y-%m-%d").txt"
echo " finish " >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group5_$(date +"%Y-%m-%d").txt"
echo $(date +"%Y-%m-%d %H:%M:%S") >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group5_$(date +"%Y-%m-%d").txt"
