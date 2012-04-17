#!/bin/sh

expectPairs() {
    COL1=$1
    START=$2
    END=$3
    for j in `seq $START $END`; do
        echo "$COL1 $j"
    done
}

check() {
    if cmp rcfr-expect.txt rcfr-got.txt; then
        echo "ok on $1"
    else
        echo "error on $1 expect vs got:"
        diff -y rcfr-expect.txt rcfr-got.txt
        exit 1
    fi
}

toTxtCheck() {
    ../process/ds2txt --skip-all $1 >rcfr-got.txt
    check $2
}

SEQ=`which seq 2>/dev/null`
if [ -z "$SEQ" ]; then
    seq() {
        seq_i=$1
        while [ $seq_i -le $2 ]; do
            echo $seq_i
            seq_i=`expr $seq_i + 1`
        done
    }
fi

echo "--------------- testing simple file rotation via direct call ----------"
rm simple-fr-*.ds
./file-rotation simple
start=0
for i in `seq 0 9`; do
    end=`expr $start + $i + 4`
    expectPairs $i $start $end >rcfr-expect.txt
    toTxtCheck simple-fr-$i.ds simple-$i
    start=`expr $end + 1`
done

echo "--------------- testing simple rotation via class ----------"
./file-rotation simple-rotating
start=0
for i in `seq 0 9`; do
    [ ! -f rcfr-expect.txt ] || rm rcfr-expect.txt
    end=`expr $start + $i + 4`
    expectPairs $i $start $end >rcfr-expect.txt
    if [ $i = 0 ]; then
        # 0 is a special case because we write an extent before rotating
        start=`expr $end + 1`
        end=`expr $start + 4`
        expectPairs $i $start $end >>rcfr-expect.txt
    fi
    if [ $i -lt 9 ]; then
        start=`expr $end + 1`
        end=`expr $start + $i + 5`
        expectPairs `expr $i + 1` $start $end >>rcfr-expect.txt
    fi
    toTxtCheck simple-rfs-$i.ds simple-rotating-$i
    start=`expr $end + 1`
done

echo "--------------- testing parallel rotation via thread ---------"
./file-rotation periodic-threaded-rotater --nthreads=40 --execution-time=2 --rotate-interval=0.25 \
    --extent-interval=0.05 >rcfr-ptr-out.txt

ROTATE_COUNT=`grep 'INFO: thread rotated ' rcfr-ptr-out.txt | awk '{print $4}'`
[ ! -z "$ROTATE_COUNT" ]
ROTATE_MAX=`expr $ROTATE_COUNT - 1`

[ ! -f ptr-$ROTATE_COUNT.ds ] || exit 1 # max + 1 should not exist.
# expect $ROTATE_COUNT ds files (usually 8)
rm rcfr-unsorted.txt rcfr-expect.txt >/dev/null
for i in `seq 0 $ROTATE_MAX`; do
    if [ ! -f ptr-$i.ds ]; then
        echo "Error: missing ptr-$i.ds"
        exit 1
    fi
    ../process/ds2txt --skip-all ptr-$i.ds >>rcfr-unsorted.txt
done

# assert that we get all the expected entries in the expected order.
sort -s -n -k 1,1 rcfr-unsorted.txt >rcfr-got.txt
for i in `seq 0 39`; do
    expectPairs $i 0 39 >>rcfr-expect.txt
done
check periodic-threaded-rotater

echo "--------------- testing parallel rotation via callback ---------"
./file-rotation extent-callback-rotater --nthreads=40 --execution-time=2 --rotate-interval=0.25 \
    --extent-interval=0.05 >rcfr-parallel-out.txt

ROTATE_COUNT=`grep 'INFO: rotated ' rcfr-parallel-out.txt | awk '{print $3}'`
ROTATE_MAX=`expr $ROTATE_COUNT - 1`

[ ! -f cbr-$ROTATE_COUNT.ds ] || exit 1 # max + 1 should not exist.
# expect $ROTATE_COUNT ds files (usually 8).
rm rcfr-unsorted.txt rcfr-expect.txt
for i in `seq 0 $ROTATE_MAX`; do
    if [ ! -f cbr-$i.ds ]; then
        echo "Error: missing cbr-$i.ds"
        exit 1
    fi
    ../process/ds2txt --skip-all cbr-$i.ds >>rcfr-unsorted.txt
done

# assert that we get all the expected entries in the expected order.
sort -s -n -k 1,1 rcfr-unsorted.txt >rcfr-got.txt
for i in `seq 0 39`; do
    expectPairs $i 0 39 >>rcfr-expect.txt
done
check extent-callback-rotater

echo "--------------- cleaning up leftover files ---------"
rm rcfr-*txt
rm cbr*.ds ptr-*.ds simple-*.ds
