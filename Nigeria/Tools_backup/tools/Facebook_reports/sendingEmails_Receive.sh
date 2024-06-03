
ssh edge01002 " echo -e 'Hello Team,\n\nKindly note the file received status for the feed: $1 \n\nLigaData Support Team \n' | mailx -a $2 -r 'DAAS_note_ng@mtn.com' -s 'DAAS_Note_MTN_NG_<Facebook Reporting : Files Status - $(date +"%Y-%m-%d")>' '$3'"
