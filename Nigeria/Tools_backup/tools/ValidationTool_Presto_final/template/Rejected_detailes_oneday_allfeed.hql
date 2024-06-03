insert into audit.files_count_rejected_detailes 
 SELECT filename, 
 count (*),main_reason,RejectionLoadedDate,feed_name  
 FROM (select distinct substring(fsys_msgtype,18) feed_name,fsys_filename filename ,fsys_msgnumber linenumber, main_reason from feeds.rejecteddata where file_date=RejectionLoadedDate ) a 
 GROUP BY filename,feed_name, main_reason;