rundate=$1
cd /mnt/beegfs_bsl/scripts/flytxt

date
nohup bash flytxt_spool.sh RBT_EVENT $rundate > /dev/null &
date
nohup bash flytxt_spool.sh VAS_EVENT $rundate > /dev/null &
date
nohup bash flytxt_spool.sh PACK_SUB_EVNT $rundate > /dev/null &
date
nohup bash flytxt_spool.sh REFILL_EVENT $rundate > /dev/null &
date
nohup bash flytxt_spool.sh USG_REALTIME $rundate > /dev/null &
date
nohup bash flytxt_spool.sh USG_DATA_SMS $rundate > /dev/null &
date
nohup bash flytxt_spool.sh USG_VOICE_OG $rundate > /dev/null &
date
nohup bash flytxt_spool.sh USG_VOICE_IC $rundate > /dev/null &
date
nohup bash flytxt_spool.sh REVENUE $rundate > /dev/null &
date
nohup bash flytxt_spool.sh PROFILE_WEEK $rundate > /dev/null &
date
nohup bash flytxt_spool.sh PROFILE_ADAIL $rundate > /dev/null &
date
nohup bash flytxt_spool.sh PROFILE_BDAIL $rundate > /dev/null &
date

exit
