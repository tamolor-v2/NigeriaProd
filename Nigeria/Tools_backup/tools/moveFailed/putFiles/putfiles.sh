#! /bin/bash
date=$(date -d "$1 -1 days" +'%Y%m%d')
echo "$date"
i=0
while read line
do
    array[ $i ]="$line"        
    (( i++ ))
done < <(find ~ -maxdepth 1 -type f -name "*${date}*")

echo ${array[1]}
