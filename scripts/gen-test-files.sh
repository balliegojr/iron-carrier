#!/usr/bin/env bash
set -euo pipefail

if [ $# -lt 2 ]; then
    echo "Usage: $0 <node> <target-directory>"
    exit 1
fi

NODE="$1"
TARGET="$2"
mkdir -p "$TARGET"

rand_name() {
    head -c 8 /dev/urandom | base64 | tr -dc 'a-z0-9' | head -c 8
}

gen_file() {
    local path="$1"
    local size=$(( RANDOM % 1537 + 512 ))  # 512–2048 bytes (0.5–2 kb)
    dd if=/dev/urandom of="$path" bs="$size" count=1 2>/dev/null
}

gen_dir_files() {
    local dir="$1"
    local count=$(( RANDOM % 4 + 2 ))  # 2–5 files

    gen_file "$dir/$(rand_name).ig"
    gen_file "$dir/$(rand_name).ig"

    local exts=(txt dat bin log cfg)
    for i in $(seq 3 "$count"); do
        local ext="${exts[RANDOM % ${#exts[@]}]}"
        gen_file "$dir/$(rand_name).$ext"
    done
}

# .ignore with 3 glob patterns
printf 'ignored/**\n*.ig\n**/*.ig\n' > "$TARGET/.ignore"

# ignored/ directory with 5–10 files
mkdir -p "$TARGET/ignored"
for i in $(seq 1 "$(( RANDOM % 6 + 5 ))"); do
    gen_file "$TARGET/ignored/$(rand_name).dat"
done

# file tree up to 3 levels deep
for i in $(seq 1 "$(( RANDOM % 3 + 2 ))"); do
    d1="$TARGET/$NODE/dir_$i"
    mkdir -p "$d1"
    gen_dir_files "$d1"

    for j in $(seq 1 "$(( RANDOM % 3 ))"); do
        d2="$d1/sub_$j"
        mkdir -p "$d2"
        gen_dir_files "$d2"

        for k in $(seq 1 "$(( RANDOM % 2 ))"); do
            d3="$d2/deep_$k"
            mkdir -p "$d3"
            gen_dir_files "$d3"
        done
    done
done
