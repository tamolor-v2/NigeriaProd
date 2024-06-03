start_date=20141120
num_days=5
#for i in `seq 1 $num_days`
#do
   #  date=$(date +%Y%m%d -d "${start_date}-${i} days")
    #  echo $date # Use this however you want!
	bash /mnt/beegfs_bsl/tools/ExtractTools/tbl_imei_registration_dtls_vw/tbl_imei_registration_dtls_vw.sh 20181229
        rmdir /mnt/beegfs/live/tbl_imei_registration_dtls_vw/incoming/20181229
        mv /mnt/beegfs_bsl/tools/ExtractTools/tbl_imei_registration_dtls_vw/tmp/20181229 /mnt/beegfs/live/tbl_imei_registration_dtls_vw/incoming/

        bash /mnt/beegfs_bsl/tools/ExtractTools/tbl_imei_registration_dtls_vw/tbl_imei_registration_dtls_vw.sh 20181230
        rmdir /mnt/beegfs/live/tbl_imei_registration_dtls_vw/incoming/20181230
        mv /mnt/beegfs_bsl/tools/ExtractTools/tbl_imei_registration_dtls_vw/tmp/20181230/mnt/beegfs/live/tbl_imei_registration_dtls_vw/incoming/

        bash /mnt/beegfs_bsl/tools/ExtractTools/tbl_imei_registration_dtls_vw/tbl_imei_registration_dtls_vw.sh 20181231
        rmdir /mnt/beegfs/live/tbl_imei_registration_dtls_vw/incoming/20181231
        mv /mnt/beegfs_bsl/tools/ExtractTools/tbl_imei_registration_dtls_vw/tmp/20181231 /mnt/beegfs/live/tbl_imei_registration_dtls_vw/incoming/

        bash /mnt/beegfs_bsl/tools/ExtractTools/tbl_imei_registration_dtls_vw/tbl_imei_registration_dtls_vw.sh 20190101
        rmdir /mnt/beegfs/live/tbl_imei_registration_dtls_vw/incoming/20190101
        mv /mnt/beegfs_bsl/tools/ExtractTools/tbl_imei_registration_dtls_vw/tmp/20190101 /mnt/beegfs/live/tbl_imei_registration_dtls_vw/incoming/

        bash /mnt/beegfs_bsl/tools/ExtractTools/tbl_imei_registration_dtls_vw/tbl_imei_registration_dtls_vw.sh 20190102
        rmdir /mnt/beegfs/live/tbl_imei_registration_dtls_vw/incoming/20190102
        mv /mnt/beegfs_bsl/tools/ExtractTools/tbl_imei_registration_dtls_vw/tmp/20190102 /mnt/beegfs/live/tbl_imei_registration_dtls_vw/incoming/

        bash /mnt/beegfs_bsl/tools/ExtractTools/tbl_imei_registration_dtls_vw/tbl_imei_registration_dtls_vw.sh 20190103
        rmdir /mnt/beegfs/live/tbl_imei_registration_dtls_vw/incoming/20190103
        mv /mnt/beegfs_bsl/tools/ExtractTools/tbl_imei_registration_dtls_vw/tmp/20190103 /mnt/beegfs/live/tbl_imei_registration_dtls_vw/incoming/

        bash /mnt/beegfs_bsl/tools/ExtractTools/tbl_imei_registration_dtls_vw/tbl_imei_registration_dtls_vw.sh 20190104
        rmdir /mnt/beegfs/live/tbl_imei_registration_dtls_vw/incoming/20190104
        mv /mnt/beegfs_bsl/tools/ExtractTools/tbl_imei_registration_dtls_vw/tmp/20190104 /mnt/beegfs/live/tbl_imei_registration_dtls_vw/incoming/

        bash /mnt/beegfs_bsl/tools/ExtractTools/tbl_imei_registration_dtls_vw/tbl_imei_registration_dtls_vw.sh 20190105
        rmdir /mnt/beegfs/live/tbl_imei_registration_dtls_vw/incoming/20190105
        mv /mnt/beegfs_bsl/tools/ExtractTools/tbl_imei_registration_dtls_vw/tmp/20190105 /mnt/beegfs/live/tbl_imei_registration_dtls_vw/incoming/

        bash /mnt/beegfs_bsl/tools/ExtractTools/tbl_imei_registration_dtls_vw/tbl_imei_registration_dtls_vw.sh 20190106
        rmdir /mnt/beegfs/live/tbl_imei_registration_dtls_vw/incoming/20190106
        mv /mnt/beegfs_bsl/tools/ExtractTools/tbl_imei_registration_dtls_vw/tmp/20190106 /mnt/beegfs/live/tbl_imei_registration_dtls_vw/incoming/

     
#done
