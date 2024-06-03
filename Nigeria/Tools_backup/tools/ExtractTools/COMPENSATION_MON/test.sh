nowmonth=$(date +%Y-%m)
mn_tun=$(date -d "$nowmonth-15 last month" '+%Y%m')
echo $mn_tun

