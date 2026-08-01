#!/usr/bin/env bash
set -uo pipefail

if [ $# -lt 3 ]; then
    echo "Usage: $0 <target-directory> <duration-seconds> <dirs|truncate|updates>"
    exit 1
fi

TARGET_DIR="$1"
DURATION="$2"
MODE="$3"

rand_name() {
    head -c 8 /dev/urandom | base64 | tr -dc 'a-z0-9' | head -c 8
}

gen_file() {
    local path="$1"
    local size=$(( RANDOM % 1537 + 512 ))  # 512-2048 bytes
    dd if=/dev/urandom of="$path" bs="$size" count=1 2>/dev/null
}

candidate_files() {
    find "$TARGET_DIR" -type f \
        -not -path "$TARGET_DIR/ignored/*" \
        -not -name "*.ig" \
        -not -name ".ignore"
}

candidate_dirs() {
    find "$TARGET_DIR" -mindepth 1 -type d \
        -not -path "$TARGET_DIR/ignored" \
        -not -path "$TARGET_DIR/ignored/*"
}

pick_random_line() {
    shuf -n 1
}

do_dirs_op() {
    local op=$(( RANDOM % 4 ))
    case "$op" in
        0)
            local dir
            dir=$(candidate_dirs | pick_random_line)
            [ -n "$dir" ] && gen_file "$dir/$(rand_name).txt"
            ;;
        1)
            local dir newdir
            dir=$(candidate_dirs | pick_random_line)
            if [ -n "$dir" ]; then
                newdir="$dir/$(rand_name)"
                mkdir -p "$newdir"
                gen_file "$newdir/$(rand_name).txt"
            fi
            ;;
        2)
            local f
            f=$(candidate_files | pick_random_line)
            [ -n "$f" ] && rm -f "$f"
            ;;
        3)
            local f dir
            f=$(candidate_files | pick_random_line)
            dir=$(candidate_dirs | pick_random_line)
            if [ -n "$f" ] && [ -n "$dir" ]; then
                mv "$f" "$dir/$(rand_name).txt" 2>/dev/null || true
            fi
            ;;
    esac
}

do_truncate_op() {
    local f cursize newsize
    f=$(candidate_files | pick_random_line)
    [ -n "$f" ] || return 0
    cursize=$(stat -c %s "$f" 2>/dev/null || echo 0)
    [ "$cursize" -gt 0 ] || return 0
    newsize=$(( RANDOM % cursize ))
    truncate -s "$newsize" "$f"
}

do_updates_op() {
    local f op size
    f=$(candidate_files | pick_random_line)
    [ -n "$f" ] || return 0
    op=$(( RANDOM % 2 ))
    if [ "$op" -eq 0 ]; then
        gen_file "$f"
    else
        size=$(( RANDOM % 513 + 128 ))
        dd if=/dev/urandom bs="$size" count=1 >> "$f" 2>/dev/null
    fi
}

END=$(( $(date +%s) + DURATION ))

while [ "$(date +%s)" -lt "$END" ]; do
    case "$MODE" in
        dirs) do_dirs_op ;;
        truncate) do_truncate_op ;;
        updates) do_updates_op ;;
        *)
            echo "Unknown mode: $MODE" >&2
            exit 1
            ;;
    esac

    sleep "0.$(( RANDOM % 5 + 3 ))"
done
