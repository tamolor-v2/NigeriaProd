 #!/bin/bash
d="/mnt/beegfs/FlareData/CDR/*/incoming"
file_lst=($(find  ${d} -mindepth 1 -type d -empty  ))
echo "${file_lst[@]}"
