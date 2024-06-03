

d=$1   #The start date (Monday)
ed=$2 #The number of needed weeks   
days=7  
fd=$((ed * days)) #To calculate the dates of the needed weeks 
xx=$(date -d "$d -$(date -d $d +%w) days + 1 day") #To get the first Monday date
x=$(date -d "$xx"  +'%Y%m%d') #To format the date 
endDate=$(date -d "$x + ${fd} day" +'%Y%m%d')
echo -e "Started date -- $x\nEndDate --  $endDate"
while [ "$x" -lt $endDate ]; do
st=$(date +"%Y%m%d%H%M%S")
echo "Started for $x at $st"
#bash /nas/share05/tools/CVM_Reports/Insert_CVM20_REFILL_AND_SUBSCRIPTION_BACTH2.sh $x
x=$(date -d "$x + 7 day" +'%Y%m%d')
done
