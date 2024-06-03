file="/mnt/beegfs_bsl/tools/ExtractTools/NEWREG_BIOUPDT_POOL_LIVE/staging/maxSeq.txt"
if [ ! -f "$file" ]
then
        maxSeq=0
        echo "0" >/mnt/beegfs_bsl/tools/ExtractTools/NEWREG_BIOUPDT_POOL_LIVE/staging/maxSeq.txt
else
#       maxSeq=$(</mnt/beegfs_bsl/tools/ExtractTools/NEWREG_BIOUPDT_POOL_LIVE/staging/maxSeq.txt)
        maxSeq=$( tail -n 1 /mnt/beegfs_bsl/tools/ExtractTools/NEWREG_BIOUPDT_POOL_LIVE/staging/maxSeq.txt)
fi
echo "Max Seq No from last run=$maxSeq"
echo "started spooling $filename"


