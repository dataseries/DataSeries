#!/bin/sh

# 100MB = 35 copies of the bible
./grepanalysis.py prepare --copies=35 /usr/share/datasets/bible.txt /tmp/grep
./grepanalysis.py experiment --tag="gensort 100MB" --iterations=3 "snakes" /tmp/grep

# 1GB = 350 copies of the bible
./grepanalysis.py prepare --copies=350 /usr/share/datasets/bible.txt /tmp/grep
./grepanalysis.py experiment --tag="gensort 1GB" --iterations=3 "snakes" /tmp/grep

# 10GB = 3500 copies of the bible
./grepanalysis.py prepare --copies=3500 /usr/share/datasets/bible.txt /tmp/grep
./grepanalysis.py experiment --tag="gensort 10GB" --iterations=3 "snakes" /tmp/grep

rm /tmp/grep*