dt=$1
echo "$dt"
for file in "/home/daasuser/samer_scripts/vtu_script"/*
        do
#           echo $file
st_start=${file:0:40}
mm=${file:40:2}
dd=${file:42:2}
yy=${file:44:4}
rest=${file:48:200}
#echo $yy$mm$dd
#echo "$yy-$mm-$yy       $rest"
newFileName=$st_start$yy$mm$dd$rest
echo "$file -----------------------> $st_start$yy$mm$dd$rest"
mv $file $newFileName
        done
#processing_dt=$(date --date $dt +"%Y%m%d")
#echo $processing_dt

