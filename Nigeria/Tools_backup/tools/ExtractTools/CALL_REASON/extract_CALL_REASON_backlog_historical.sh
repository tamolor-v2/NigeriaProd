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
    bash /nas/share05/tools/ExtractTools/CALL_REASON/extract_CALL_REASON_backlog.sh  "0 1 2 3 4" $d
    bash /nas/share05/tools/ExtractTools/CALL_REASON/extract_CALL_REASON_backlog.sh  "5 6 7 8 9" $d
done
