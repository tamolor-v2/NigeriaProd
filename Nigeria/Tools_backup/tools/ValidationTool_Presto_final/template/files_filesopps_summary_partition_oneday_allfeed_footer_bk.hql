else '1917' end file_dt from audit.files_filesopps_summary where lines > 0 and tbl_dt between yesterday and nextday) FilesOps where file_dt like 'targetdate';
