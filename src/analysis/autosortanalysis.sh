#!/bin/sh

# 100MB = 1000000 records
./sortanalysis.py prepareGensort --records=1000000 /tmp/gensort
./sortanalysis.py experiment --tag="gensort 100MB" --gensort --iterations=1 --extentLimit=200000 --memoryLimit=2000000000 --compressOutput? --compressTemp? /tmp/gensort /tmp/gensort.out

# 1GB = 10000000 records
./sortanalysis.py prepareGensort --records=10000000 /tmp/gensort
./sortanalysis.py experiment --tag="gensort 1GB" --gensort --iterations=1 --extentLimit=200000 --memoryLimit=2000000000 --compressOutput? --compressTemp? /tmp/gensort /tmp/gensort.out

# 10GB = 100000000 records
./sortanalysis.py prepareGensort --records=100000000 /tmp/gensort
./sortanalysis.py experiment --tag="gensort 10GB" --gensort --iterations=1 --extentLimit=200000,2000000 --memoryLimit=2000000000 --compressOutput? --compressTemp? /tmp/gensort /tmp/gensort.out

rm /tmp/gensort*