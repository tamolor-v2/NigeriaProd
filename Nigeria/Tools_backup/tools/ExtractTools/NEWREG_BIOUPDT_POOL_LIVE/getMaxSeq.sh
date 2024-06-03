maxSeq=$(tail -1  spool/NEWREG_BIOUPDT_POOL_LIVE_20181213.csv |awk -F '|' '{print $1}')
echo $maxSeq
