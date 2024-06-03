startdate=$1
yesterday=$( date -d "${startdate} -1 days" +'%Y%m%d' )
enddate=$2
d=
n=0
until [ "$d" = "$enddate" ]
do
    ((n++))
    d=$(date -d "$yesterday + $n days" +%Y%m%d)
    echo $d
    bash /nas/share05/tools/ExtractTools/MNP_PORTING_BROADCAST/extract_MNP_PORTING_BROADCAST_rerun_date_range.sh $d
done
