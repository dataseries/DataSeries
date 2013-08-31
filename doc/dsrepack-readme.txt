These files are encoded for space efficiency, not speed of access.
After downloading these files, a repack step to a more speed efficient
compression algorithm will most likely improve processing performance
SIGNIFICANTLY.

There are currently seven compression algorithms supported by
DataSeries: BZIP2, GZIP, LZO, LZF, SNAPPY, LZ4, and LZ4HC.  Their
performance with respect to different metrics is summarized below:

Algorithm   Compression Speed	 Decompression Speed	File Size
LZO	    4th best (slowest)	 fastest		3rd best (moderate)
LZF	    fastest		 fast			4th best (large)
GZIP	    3rd best (at -9)	 moderate		2nd best
BZIP2	    2nd best		 slow			smallest

Snappy, lz4, and lz4hc have not been extensively tested, but based on
preliminary results the relative performances are as follows:

Algorithm   Compression Speed	 Decompression Speed	File Size
SNAPPY      2nd best		 best (fastest)         5th best
LZ4         best (fastest)	 2nd best		7th best (biggest)
LZO	    7th best (slowest)	 3rd best		3rd best
LZF         3rd best 		 4th best		6th best
LZ4HC	    4th best		 5th best		4th best
GZIP	    6th best (at -9)	 6th best 		2nd best
BZIP2	    5th best 	 	 7th best (slowest)	best (smallest)

Lintel provides a processing tool called batch-parallel which will
utilize all available CPUs on a multi-core machine to speed this
process. Executing batch-parallel --man will show usage.  The module
you will want to use is dsrepack.

Running batch-parallel without any arguments will print out the
available batch-parallel modules.  If dsrepack is not one of those
modules, look in your DataSeries install root for share/bp_modules.
The dsrepacking arguments to batch-parallel on a local machine are:

batch-parallel dsrepack compress=lzo transform='s/-bz2\.ds$/-lzo.ds/' -- .

If you have an LSF cluster available to you, you can parallelize
transcoding further using batch-parallel to submit repacking jobs to
the cluster.

Batch parallel currently does not support snappy, lz4, or lz4hc

Alternatively if you have your own parallelization mechanism, the
DataSeries usage is:

% dsrepack --help
Usage: dsrepack [common-args] [--verbose] [--target-file-size=MiB] input-filename... \
output-filename

Common args:
    --{disable,compress,enable} {lzf,lzo,gz,bz2,snappy,lz4,lz4hc} (default --enable-*)
    --compress none --compress-level=[0-9] (default 9)
    --extent-size=[>=1024] (default 16*1024*1024 if bz2 is enabled, \
64*1024 otherwise)

So, an example would be:
dsrepack --compress lzo a-bz2.ds a-lzo.ds

dsrepack --compress none --enable snappy lz4 lzf input.ds output.ds

If you are enterprising, please modify batch-parallel directly to add
support for your batch-queue manager and submit it as a patch to
DataSeries.
