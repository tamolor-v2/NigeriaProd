nowmonth=$(date +%Y-%m_%d)
#previous=$(date -d "$nowmonth-15 last month" '+%Y%m%d')
#previous=$(date -d "$(date +%Y-%m-1-%d) -1 month" +%Y%m%d)
#previous=$(date -d "$(date +%Y-%m-01-%d)   " "+%Y%m%d")
Firstday=$(date -d "-1 month -$(($(date +%d)-1)) days" +%Y%m%d)
echo "${Firstday}"
