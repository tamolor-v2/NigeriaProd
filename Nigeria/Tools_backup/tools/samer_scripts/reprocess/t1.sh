                        spool_file_count=($(find  /mnt/beegfs/FlareData/CDR/MSC/incoming/20180211 -type f -name "${dt}*" |wc -l))
                        processed_file_count=($(find  /mnt/beegfs/live_bib/spool/MSC_CDR/20180225 -type f -name "${dt}*" |wc -l))
			echo "$spool_file_count : $processed_file_count"
			if [ $spool_file_count>0 ] && [ $processed_file_count > 0 ] 
			then 
				echo "EXIT"
			else
				echo "Processing"
			fi  	
                        

