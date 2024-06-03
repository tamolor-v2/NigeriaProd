
ssh edge01002 " echo -e 'Hello Team,\n\nKindly note the validation status for the report : $1\n\nTotal Records : $2\nPartition date: $3 \n\nRegards,\nLigaData Support Team \n' | mailx -r 'DAAS_note_ng@mtn.com' -s 'DAAS_Note_MTN_NG_<Facebook Reporting : Validation Report - $(date +"%Y-%m-%d")>' '$4'"
