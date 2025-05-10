#!/usr/bin/env bash

set -euo pipefail
IFS=$'\n\t'

output_file="result.actual.txt"
# Usage: ./process_words.sh file1.txt file2.txt ...

if [ "$#" -eq 0 ]; then
  echo "Usage: $0 file1.txt file2.txt ..." >&2
  exit 1
fi

for file in "$@"; do
  if [ ! -r "$file" ]; then
    echo "Error: File '$file' does not exist or is not readable." >&2
    exit 1
  fi

  last_char=$(tail -c 1 "$file")
  if [ "$last_char" != "" ] && [ "$last_char" != $'\n' ]; then
    echo >> "$file"
    echo "Added newline to '$file'"
  fi
done

cat "$@" | \
tr '[:upper:]' '[:lower:]' | \
tr -s '[:space:]' '\n' | \
LC_ALL=C sort --parallel=12 -u --output="$output_file"

echo "Processing complete. Results written to '$output_file'."

