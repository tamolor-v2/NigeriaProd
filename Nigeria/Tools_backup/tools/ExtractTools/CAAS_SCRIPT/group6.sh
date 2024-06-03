echo ""  >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group6_$(date +"%Y-%m-%d").txt"
echo "\n start date \n" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group6_$(date +"%Y-%m-%d").txt"
echo $(date +"%Y-%m-%d %H:%M:%S") >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group6_$(date +"%Y-%m-%d").txt"
#echo  "start SPECIAL_DATA" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group6_$(date +"%Y-%m-%d").txt"
#bash /nas/share05/tools/ExtractTools/SPECIAL_DATA/SPECIAL_DATA.sh
#echo  "end SPECIAL_DATA" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group6_$(date +"%Y-%m-%d").txt"
echo  "start WSQ_IMAGE" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group6_$(date +"%Y-%m-%d").txt"
bash /nas/share05/tools/ExtractTools/WSQ_IMAGE/WSQ_IMAGE.sh
echo  "end WSQ_IMAGE" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group6_$(date +"%Y-%m-%d").txt"
echo  "start CB_ACCOUNT_SERVICE_LIST" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group6_$(date +"%Y-%m-%d").txt"
bash /nas/share05/tools/ExtractTools/CB_ACCOUNT_SERVICE_LIST/CB_ACCOUNT_SERVICE_LIST.sh
echo  "end CB_ACCOUNT_SERVICE_LIST" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group6_$(date +"%Y-%m-%d").txt"
echo  "start TBL_ORDER_DTLS_VIEW" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group6_$(date +"%Y-%m-%d").txt"
bash /nas/share05/tools/ExtractTools/TBL_ORDER_DTLS_VIEW/TBL_ORDER_DTLS_VIEW.sh
echo  "end TBL_ORDER_DTLS_VIEW" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group6_$(date +"%Y-%m-%d").txt"
echo  "start IDENTITY_INVOICE_INFO" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group6_$(date +"%Y-%m-%d").txt"
bash /nas/share05/tools/ExtractTools/IDENTITY_INVOICE_INFO/IDENTITY_INVOICE_INFO.sh
echo  "end IDENTITY_INVOICE_INFO" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group6_$(date +"%Y-%m-%d").txt"
echo  "start BANK_GUARANTEE" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group6_$(date +"%Y-%m-%d").txt"
bash /nas/share05/tools/ExtractTools/BANK_GUARANTEE/BANK_GUARANTEE.sh
echo  "end BANK_GUARANTEE" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group6_$(date +"%Y-%m-%d").txt"
echo  "start GEN_LED_VOUCHER_ROW" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group6_$(date +"%Y-%m-%d").txt"
bash /nas/share05/tools/ExtractTools/GEN_LED_VOUCHER_ROW/GEN_LED_VOUCHER_ROW.sh
echo  "end GEN_LED_VOUCHER_ROW" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group6_$(date +"%Y-%m-%d").txt"
echo  "start INVOICE_LEDGER_ITEM_SU_QRY" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group6_$(date +"%Y-%m-%d").txt"
bash /nas/share05/tools/ExtractTools/INVOICE_LEDGER_ITEM_SU_QRY/INVOICE_LEDGER_ITEM_SU_QRY.sh
echo  "end INVOICE_LEDGER_ITEM_SU_QRY" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group6_$(date +"%Y-%m-%d").txt"
echo  "start INVOICE" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group6_$(date +"%Y-%m-%d").txt"
bash /nas/share05/tools/ExtractTools/INVOICE/INVOICE.sh
echo  "end INVOICE" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group6_$(date +"%Y-%m-%d").txt"
echo  "start LEDGER_ITEM" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group6_$(date +"%Y-%m-%d").txt"
bash /nas/share05/tools/ExtractTools/LEDGER_ITEM/LEDGER_ITEM.sh
echo  "end LEDGER_ITEM" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group6_$(date +"%Y-%m-%d").txt"

