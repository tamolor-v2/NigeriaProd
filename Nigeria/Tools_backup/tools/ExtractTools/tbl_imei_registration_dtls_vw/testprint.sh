yyyymmdd=`date  "+%Y%m%d"`
yest=$(date -d "-1 day" '+%Y%m%d')
filename=$yest"_"$yyyymmdd"_tbl_imei_registration_dtls_vw.csv"

echo ${filename}
