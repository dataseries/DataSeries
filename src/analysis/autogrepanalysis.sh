#!/bin/sh
[ "$DATASETS" = "" ] && DATASETS=/usr/share/datasets
# TODO-tomer: move into DataSeries/experiments/grep.
# document where I get bible.txt

if [ ! -r $DATASETS/bible.txt ]; then
    echo "Missing $DATASETS/bible.txt, you can get it from ..."
    exit 1
fi
# 100MB = 35 copies of the bible
./grepanalysis.py prepare --copies=35 $DATASETS/bible.txt /tmp/grep
./grepanalysis.py experiment --tag="gensort 100MB" --iterations=3 "snakes" /tmp/grep

# 1GB = 350 copies of the bible
./grepanalysis.py prepare --copies=350 $DATASETS/bible.txt /tmp/grep
./grepanalysis.py experiment --tag="gensort 1GB" --iterations=3 "snakes" /tmp/grep

# 10GB = 3500 copies of the bible
./grepanalysis.py prepare --copies=3500 $DATASETS/bible.txt /tmp/grep
./grepanalysis.py experiment --tag="gensort 10GB" --iterations=3 "snakes" /tmp/grep

rm /tmp/grep*