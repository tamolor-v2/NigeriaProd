rundate=$1
cd /mnt/beegfs_bsl/scripts/flytxt

date
nohup bash flytxt_spool_adt.sh RBT_EVENT $rundate > /dev/null &
date
nohup bash flytxt_spool_adt.sh VAS_EVENT $rundate > /dev/null &
date
nohup bash flytxt_spool_adt.sh PACK_SUB_EVNT $rundate > /dev/null &
date
nohup bash flytxt_spool_adt.sh REFILL_EVENT $rundate > /dev/null &
date
nohup bash flytxt_spool_adt.sh USG_REALTIME $rundate > /dev/null &
date
nohup bash flytxt_spool_adt.sh USG_DATA_SMS $rundate > /dev/null &
date
nohup bash flytxt_spool_adt.sh USG_VOICE_OG $rundate > /dev/null &
date
nohup bash flytxt_spool_adt.sh USG_VOICE_IC $rundate > /dev/null &
date
nohup bash flytxt_spool_adt.sh REVENUE $rundate > /dev/null &
date
nohup bash flytxt_spool_adt.sh PROFILE_WEEK $rundate > /dev/null &
date
nohup bash flytxt_spool_adt.sh PROFILE_ADAIL $rundate > /dev/null &
date
nohup bash flytxt_spool_adt.sh PROFILE_BDAIL $rundate > /dev/null &
date

exit
