These files are encoded for space efficiency, not speed of access.
After downloading these files, a repack step to a more speed efficient
compression algorithm will most likely improve processing performance
SIGNIFICANTLY.

There are currently four compression algorithms supported by
DataSeries: BZIP2, GZIP, LZO and LZF.  Their performance with respect
to different metrics is summarized below:

Algorithm   Compression Speed	 Decompression Speed	File Size
LZO	    4th best (slowest)	 fastest		3rd best (moderate)
LZF	    fastest		 fast			4th best (large)
GZIP	    3rd best (at -9)	 moderate		2nd best
BZIP2	    2nd best		 slow			smallest


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

Alternatively if you have your own parallelization mechanism, the
DataSeries usage is:

% dsrepack --help
Usage: dsrepack [common-args] [--target-file-size=MiB] input-filename... \
output-filename

Common args:
    --{disable,compress,enable}-{lzf,lzo,gz,bz2} (default --enable-*)
    --compress-none --compress-level=[0-9] (default 9)
    --extent-size=[>=1024] (default 16*1024*1024 if bz2 is enabled, \
64*1024 otherwise)

So, an example would be:
dsrepack --compress-lzo a-bz2.ds a-lzo.ds

If you are enterprising, please modify batch-parallel directly to add
support for your batch-queue manager and submit it as a patch to
DataSeries.
