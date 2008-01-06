#
#  (c) Copyright 2005-2007, Hewlett-Packard Development Company, LP
#
#  See the file named COPYING for license details
#

package BatchParallel::ellardnfs2ds;

use File::Compare;
use FileHandle;
use strict;
use vars '@ISA';

@ISA = qw/BatchParallel::common/;

sub new {
    my $class = shift;

    my $this = bless {'check' => 1}, $class;
    while (@_ > 0) {
	$_ = shift @_;
	if ($_ eq 'help') {
	    $this->usage();
	    exit(0);
	} elsif (/^compress=(.+)$/o) {
	    die "Already specified compression level" 
		if defined $this->{compress};
	    $this->{compress} = $1;
	} elsif (/^extent-size=(\d+(\.\d+)?)([km])?$/o) {
	    die "Can't set multiple extent sizes" 
		if defined $this->{extent_size};
	    my ($size,$units) = ($1,$3);
	    $units ||= '';
	    $size *= 1024 if $units eq 'k';
	    $size *= 1024 * 1024 if $units eq 'm';
	    $size = sprintf("%d", $size);
	    $this->{extent_size} = "--extent-size=$size";
        } elsif (/^nocheck$/o) {
	    $this->{check} = 0;
	} else {
	    die "unknown options specified for batch-parallel module $class: '@_'";
	}
    }
    return $this;
}

sub usage {
    print "batch-parallel ellardnfs2ds [compress={bz2,lzf,gz,lzo}] [extent-size=#.#[km]] [nocheck] -- file/directory...\n";
}

sub file_is_source {
    my($this,$prefix,$fullpath,$filename) = @_;

    return 1 if $fullpath =~ /\.txt.gz$/o;
    return 0;
}

sub destination_file {
    my($this,$prefix,$fullpath) = @_;

    $fullpath =~ s/\.txt\.gz$/.ds/o;
    return $fullpath;
}

sub rebuild {
    my($this,$prefix,$fullpath,$destpath) = @_;

    my @cmd = ("gunzip -c < $fullpath |", 'ellardnfs2ds');
    if (defined ($this->{compress})) {
	push(@cmd, "--compress-$this->{compress}");
    }
    if (defined ($this->{extent_size})) {
	push(@cmd, $this->{extent_size});
    }
    push(@cmd, '-', $destpath);

    my $cmd = join(" ", @cmd);
    print "$cmd\n";
    my $ok = system($cmd) == 0;
    unless($ok) {
	print "Command '$cmd' failed\n";
	return 0;
    }
    return 1 unless $this->{check};

    my $checkpath = "$destpath-check";
    # gzip to make it smaller, not for comparison, can't compare two
    # gzipped files, they may have identical uncompressed contents,
    # but not be identical
    @cmd = ('ds2ellardnfs',$destpath, "|gzip >$checkpath");
    $cmd = join(" ", @cmd);
    print "$cmd\n";
    $ok = system($cmd) == 0;
    unless($ok) {
	print "Command '$cmd' failed\n";
	return 0;
    }
    print "compare $fullpath with $checkpath...\n";
    my $fh1 = new FileHandle "gunzip -c <$fullpath |"
	or die "bad: $!";
    my $fh2 = new FileHandle "gunzip -c <$checkpath |"
	or die "bad: $!";
    if (compare($fh1, $fh2) == 0) {
	unlink($checkpath);
    } else {
	print "ERROR: $fullpath and $checkpath are not identical\n";
	return 0;
    } 
    close($fh1);
    close($fh2);
    return 1;
}

1;

