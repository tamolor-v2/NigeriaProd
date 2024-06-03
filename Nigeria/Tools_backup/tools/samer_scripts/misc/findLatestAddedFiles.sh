 #!/bin/bash

DIR=$1
find "$DIR" -type d |
while read d;
do
    files=$(ls -t "$d" | sed -n '1h; $ { G; s/\n/,/g; p }')
    printf '%s,%s\n' "$d" "$files";
done
