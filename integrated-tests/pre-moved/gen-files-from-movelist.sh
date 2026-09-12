#!/bin/bash
set -e
TARGET_DIR="$1"
POSITION="$2"   # 'old' or 'new'
MOVE_LIST="$3"

while IFS=' ' read -r old_path new_path; do
    [ -z "$old_path" ] && continue
    if [ "$POSITION" = "old" ]; then
        filepath="$old_path"
    else
        filepath="$new_path"
    fi
    fullpath="${TARGET_DIR}/${filepath}"
    mkdir -p "$(dirname "$fullpath")"
    printf 'test-file-content' > "$fullpath"
    touch -t 202001010000 "$fullpath"
done < "$MOVE_LIST"
