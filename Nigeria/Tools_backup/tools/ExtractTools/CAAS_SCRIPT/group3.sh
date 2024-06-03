echo ""  >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group3_$(date +"%Y-%m-%d").txt"
echo "\n start date \n" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group3_$(date +"%Y-%m-%d").txt"
echo $(date +"%Y-%m-%d %H:%M:%S") >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group3_$(date +"%Y-%m-%d").txt"
echo  "start GEN_LED_VOUCHER" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group3_$(date +"%Y-%m-%d").txt"
bash /nas/share05/tools/ExtractTools/GEN_LED_VOUCHER/GEN_LED_VOUCHER_LIVE.sh
echo  "end GEN_LED_VOUCHER" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group3_$(date +"%Y-%m-%d").txt"
echo  "start HISTORY_LOG" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group3_$(date +"%Y-%m-%d").txt"
bash /nas/share05/tools/ExtractTools/HISTORY_LOG/HISTORY_LOG.sh
echo  "end HISTORY_LOG" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group3_$(date +"%Y-%m-%d").txt"
echo  "start INVENTOTY_PARTY_IN_STOCK" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group3_$(date +"%Y-%m-%d").txt"
bash /nas/share05/tools/ExtractTools/INVENTOTY_PARTY_IN_STOCK/INVENTOTY_PARTY_IN_STOCK.sh
echo  "end INVENTOTY_PARTY_IN_STOCK" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group3_$(date +"%Y-%m-%d").txt"
echo  "start INVENTORY_TRANSACTION_HIST" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group3_$(date +"%Y-%m-%d").txt"
bash/nas/share05/tools/ExtractTools/INVENTORY_TRANSACTION_HIST/INVENTORY_TRANSACTION_HIST.sh 
echo  "end INVENTORY_TRANSACTION_HIST" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group3_$(date +"%Y-%m-%d").txt"
echo  "start LEDGER_ITEM_TAB" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group3_$(date +"%Y-%m-%d").txt"
bash /nas/share05/tools/ExtractTools/LEDGER_ITEM_TAB/LEDGER_ITEM_TAB.sh
echo  "end LEDGER_ITEM_TAB" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group3_$(date +"%Y-%m-%d").txt"
echo  "start PAYMENT_ADDRESS_GENERAL" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group3_$(date +"%Y-%m-%d").txt"
bash /nas/share05/tools/ExtractTools/PAYMENT_ADDRESS_GENERAL/PAYMENT_ADDRESS_GENERAL.sh
echo  "end PAYMENT_ADDRESS_GENERAL" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group3_$(date +"%Y-%m-%d").txt"
echo  "start PAYMENT_TAB" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group3_$(date +"%Y-%m-%d").txt"
bash /nas/share05/tools/ExtractTools/PAYMENT_TAB/PAYMENT_TAB.sh
echo  "end PAYMENT_TAB" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group3_$(date +"%Y-%m-%d").txt"
echo  "start PAYMENT_TERM" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group3_$(date +"%Y-%m-%d").txt"
bash /nas/share05/tools/ExtractTools/PAYMENT_TERM/PAYMENT_TERM.sh
echo  "end PAYMENT_TERM" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group3_$(date +"%Y-%m-%d").txt"
echo  "start PRICE_LIST" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group3_$(date +"%Y-%m-%d").txt"
bash /nas/share05/tools/ExtractTools/PRICE_LIST/PRICE_LIST.sh
echo  "end PRICE_LIST" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group3_$(date +"%Y-%m-%d").txt"
echo  "start PURCH_REQ_APPROVAL" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group3_$(date +"%Y-%m-%d").txt"
bash /nas/share05/tools/ExtractTools/PURCH_REQ_APPROVAL/PURCH_REQ_APPROVAL.sh
echo  "end PURCH_REQ_APPROVAL" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group3_$(date +"%Y-%m-%d").txt"
echo "\n start end \n" >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group3_$(date +"%Y-%m-%d").txt"
echo $(date +"%Y-%m-%d %H:%M:%S") >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group3_$(date +"%Y-%m-%d").txt"
echo " finish " >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group3_$(date +"%Y-%m-%d").txt"
echo $(date +"%Y-%m-%d %H:%M:%S") >>  "/nas/share05/tools/ExtractTools/CAAS_LOG/stataus_group3$(date +"%Y-%m-%d").txt"
