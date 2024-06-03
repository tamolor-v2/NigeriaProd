
yest=$2

fileYear=`date +%Y`
quarter=$(( ($(date +%-m)-1)/3+1 ))

path_from=$4
path_to=$6
file_name=$8
type_name=${10}
Old_name=${12}
Opco_name=${13}
for f in $path_from/*\ *; do mv "$f" "${f// /_}"; done
x=$(ls $path_from | grep -i $type_name | grep -i $Old_name )
for f in $path_from/*\ *; do mv "$f" "${f// /_}"; done
x=$(ls $path_from | grep -i $type_name | grep -i $Old_name )
for i in $x
do
sleep 2
Daye_run=$(date -d " $yest " +'%d')
year=$(date -d " $yest " +'%Y')
month=`ls $path_from | grep -i $i | cut -f4 -d"_" | cut -f1 -d"."| rev | cut -c3- | rev`
mydate="$month $year"
current_Month=`date --date="$(printf "01 %s" $mydate)" +"%m"`
change_time=$(date -d " $current_Month " +'%Y-%m-%d')
full_date=$year-$current_Month-$Daye_run
mv ${path_from}/$i  ${path_to}/${file_name}${full_date}_CY${fileYear}Q${quarter}_${Opco_name}.${type_name}
echo ${path_to}/${file_name}${full_date}_CY${fileYear}Q${quarter}_${Opco_name}.${type_name}
done

