dt=$1
echo "$dt"
for file in "/mnt/beegfs/production/live/CS5_VTU_DUMP/processed"/*
        do
#           echo $file
st_start=${file:0:51}
mm=${file:51:2}
dd=${file:53:2}
yy=${file:55:4}
rest=${file:60:200}
#echo $yy$mm$dd
#echo "$yy-$mm-$yy       $rest"
newFileName=$st_start$yy$mm$dd$rest
echo "$file -----------------------> $st_start$yy$mm$dd$rest"
mv $file $newFileName
        done
#processing_dt=$(date --date $dt +"%Y%m%d")
#echo $processing_dt

