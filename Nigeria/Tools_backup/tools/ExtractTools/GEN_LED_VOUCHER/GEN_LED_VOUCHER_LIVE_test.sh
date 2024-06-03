
yest=$(date -d '-1 day' '+%Y%m%d')
mkdir -p /nas/share05/tools/ExtractTools/GEN_LED_VOUCHER/${yest}
chmod -R 777 /nas/share05/tools/ExtractTools/GEN_LED_VOUCHER/spool
cd /nas/share05/tools/ExtractTools/GEN_LED_VOUCHER/spool
#find -name '*.gz' -exec sh -c 'gunzip -d "${1%.*}" "$1"' _ {} \;
split -l 200000 -d --additional-suffix=.txt ${yest}_GEN_LED_VOUCHER.csv /nas/share05/tools/ExtractTools/GEN_LED_VOUCHER/${yest}/${yest}_GEN_LED_VOUCHER

