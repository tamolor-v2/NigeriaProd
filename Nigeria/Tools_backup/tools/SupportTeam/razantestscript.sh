


yest=$(date -d '-1 day' '+%Y%m%d')


/opt/presto/bin/presto --server master01004:8099 --catalog hive5 --schema flare_8 --execute " select msisdn_key from flare_8.customersubject where aggr='daily' and dola between 0 and 29 and datausage30dayskb =0  and tbl_dt =${yest} " --output-format CSV_HEADER | sed 's/[\t]/,/g' | awk '{gsub(/\"/,"")};1' > /home/rqader/Dola30_OfferID${yest}.csv

mv /mnt/beegfs/tools/SupportTeam/data/rgs/*.csv /mnt/beegfs/tools/SupportTeam/data/archive
sudo -u daasuser  chmod 777  /mnt/beegfs/tools/SupportTeam/data/archive/Dola30_OfferID${yest}.csv




