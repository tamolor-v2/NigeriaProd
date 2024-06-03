yest=$(date -d '-1 day' '+%Y%m%d')

bash /nas/share05/tools/DedupIncr/bin/DedupIncr_new_EZ.sh -f PAYMENT_GATEWAY_TRANSACTIONS -p m1004 -d  $yest -n 2 -r 2>&1 | tee /nas/share05/tools/DedupIncr/summarylogs/DedupRun_PAYMENT_GATEWAY_TRANSACTIONS_$(date +%Y%m%d%H%M%S).txt;

