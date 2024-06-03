CURRENTDATE="$(date +%Y%m%d)"
FiveDaysBefore="$(${CURRENTDATE} -d 5 days ago)"
echo $CURRENTDATE
echo $FiveDaysBefore
