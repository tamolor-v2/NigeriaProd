
ssh edge01002 " echo -e 'Hello Team,\n\nKindly note that the feed below is overdue by $2 days.\n\nFeed Name: $1 \nPartition date: $3 \n\nRegards,\nLigaData Support Team \n' | mailx -r 'DAAS_note_ng@mtn.com' -s 'DAAS_Note_MTN_NG_<Facebook Reporting : SLA Breach - $(date +"%Y-%m-%d")>' '$4'"
