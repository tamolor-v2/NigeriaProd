startdate=$1
enddate=$2

cd /mnt/beegfs_bsl/tools/ExtractTools/CB_SERV_MAST_VIEW_LIVE

#Validate if startdate is less or equal than end date
if [ $startdate -gt $enddate ]
then
echo "Startdate cannot be greater than enddate. First date parameter cannot be greater than second date parameter."
exit
fi
echo "processing:$startdate"
bash run_this.sh $startdate $enddate cb_serv_mast cb_serv_mast 

exit
