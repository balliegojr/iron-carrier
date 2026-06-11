#!/bin/bash
set -e
TARGET_DIR="$1"
FILES_LIST="$2"
MTIME="${3:-old}"   # 'old' sets mtime to Jan 2020; 'new' leaves mtime at creation time

while IFS= read -r filepath; do
    [ -z "$filepath" ] && continue
    fullpath="${TARGET_DIR}/${filepath}"
    mkdir -p "$(dirname "$fullpath")"
    dd if=/dev/urandom bs=512 count=1 2>/dev/null | base64 > "$fullpath"
    if [ "$MTIME" = "old" ]; then touch -t 202001010000 "$fullpath"; fi
done < "$FILES_LIST"
