#!/bin/sh
kinit -kt /etc/security/keytabs/daasuser.keytab daasuser@MTN.COM
q1="drop table if exists flare_8.DPIUNPACK_DAY_ROAM_URI_BSL;"
q2="create table flare_8.DPIUNPACK_DAY_ROAM_URI_BSL as select a.domain, b.country_enrich, b.operator_enrich, SUM(COALESCE(CAST(UPLINK AS INT),0)) VOLUP,  SUM(COALESCE(CAST(DOWNLINK AS INT),0)) VOLDOWN,  SUM(COALESCE(CAST(UPLINK AS INT),0)+COALESCE(CAST(DOWNLINK AS INT),0)) VOL,  ROUND(AVG(COALESCE(CAST(UPLINK AS INT),0))) AVG_VOLUP_PER_VISIT, ROUND(AVG(COALESCE(CAST(DOWNLINK AS INT),0))) AVG_VOLDOWN_PER_VISIT, ROUND(AVG(COALESCE(CAST(UPLINK AS INT),0)+COALESCE(CAST(DOWNLINK AS INT),0))) AVG_VOL_PER_VISIT, count(*) TOT_VISITS, round(count(*)/count(distinct a.msisdn_key)) AVG_VISITS, count(distinct a.msisdn_key) UNIQ_VISITORS, rank() over (order by count(*)/count(distinct a.msisdn_key) desc, AVG(COALESCE(CAST(UPLINK AS INT),0)+COALESCE(CAST(DOWNLINK AS INT),0)) desc, a.domain asc) RNK_BY_AVG_VISITS, rank() over (order by AVG(COALESCE(CAST(UPLINK AS INT),0)+COALESCE(CAST(DOWNLINK AS INT),0)) desc, count(*)/count(distinct a.msisdn_key) desc, a.domain asc) RNK_BY_AVG_VOL, rank() over (order by count(*) desc,SUM(COALESCE(CAST(UPLINK AS INT),0)+COALESCE(CAST(DOWNLINK AS INT),0)) desc, a.domain asc) RNK_BY_TOT_VISITS, rank() over (order by SUM(COALESCE(CAST(UPLINK AS INT),0)+COALESCE(CAST(DOWNLINK AS INT),0)) desc, count(*) desc,a.domain asc) RNK_BY_TOT_VOL, a.tbl_dt from flare_8.dpi_cdr_unpack a, flare_8.dpi_cdr b where a.tbl_dt=20180501 and a.tbl_dt=b.tbl_dt and a.unique_id_enrich=b.unique_id_enrich and b.mcc<>621 and a.typ='UR' group by a.tbl_dt,a.domain,b.country_enrich,b.operator_enrich"
echo "hive -e $q1"
#r1=`hive -e "$q1"`
ret=1
until [ ${ret} -eq 0 ];do 
#`hive -e "$q1"`
echo "cpopulating flare_8.DPIUNPACK_DAY_ROAM_URI_BSL"
r2=`/opt/presto/bin/presto --server master01004:8099 --catalog hive --schema flare_8 --output-format  CSV_HEADER --execute "$q2"`
ret=$?
sleep 10
done
echo "done"
