#
#  (c) Copyright 2006-2007, Hewlett-Packard Development Company, LP
#
#  See the file named COPYING for license details
#

package BatchParallel::dsrepack;
use strict;
use warnings;
use File::Path;
use FileHandle;
use vars '@ISA';
@ISA = qw/BatchParallel::common/;

sub new {
    my $class = shift;

    my $this = bless {
	'compress' => [],
	'transform' => 's!^(.*)\.ds$!$1-repack.ds!o',
    }, $class;
    while (@_ > 0) {
	$_ = shift @_;
	if ($_ eq 'help') {
	    $this->usage();
	    exit(0);
	} elsif (/^compress=((lzo)|(gz)|(bz2)|(lzf))$/o) {
	    push(@{$this->{compress}}, "--enable-$1");
	} elsif (/^extent-size=(\d+(\.\d+)?)([km])?$/o) {
	    my ($size,$units) = ($1,$3);
	    $units ||= '';
	    $size *= 1024 if $units eq 'k';
	    $size *= 1024 * 1024 if $units eq 'm';
	    $size = sprintf("%d", $size);
	    $this->{extent_size} = "--extent-size=$size";
	} elsif (/^split-size=(\d+(\.\d+)?)([kmg])?$/o) {
	    my ($size,$units) = ($1,$3);
	    $units ||= '';
	    $size *= 1024 if $units eq 'k';
	    $size *= 1024 * 1024 if $units eq 'm';
	    $size *= 1024 * 1024 * 1024 if $units eq 'g';
	    $size = sprintf("%.10g", $size / (1024*1024));
	    $this->{target_file_size} = "--target-file-size=$size";
	} elsif (/^transform=(.+)$/o) {
	    $this->{transform} = $1;
	} else {
	    die "unknown options specified for batch-parallel module $class: '@_'";
	}
    }
    if ($this->{transform} eq 'subdir') {
	die "subdir transform specified without specifying exactly one sub directory"
	    unless @{$this->{compress}} == 1;
	die "internal" unless $this->{compress}->[0] =~ /^--enable-(\w+)$/o;
	$this->{transform} = "subdir_transform('$1');";
    }
    if (@{$this->{compress}} > 0) {
	# specified compression modes, disable the unused ones.
	unshift(@{$this->{compress}}, "--compress-none");
    }
    return $this;
}

sub subdir_transform {
    my ($compress_type) = @_;

    if (m!^[^/]+$!o) {
	$_ = "$compress_type/$_";
    } elsif (s!/([^/]+)$!/$compress_type/$1!o) {
    } else {
	die "??";
    }
}

sub usage {
    print "batch-parallel dsrepack [compress={bz2,lzf,gz,lzo}] [extent-size=#[km]] 
  [transform={perl-expr}|subdir] -- file/directory...
  Default transform is to add -repack to the name before the .ds
";
}

sub transform {
    my($this, $inname) = @_;

    local $_;
    $_ = $inname;
    eval $this->{transform};
    die "Transforming $inname failed: $@\nTransform was: $this->{transform}\n " 
	if $@;
    return $_;
}

sub determine_things_to_build {
    my($this, $possibles) = @_;
    
    # Ignore any file which is a destination file of another file we're processing.
    my %destfile;
    map { my $x = $this->transform($_->[1]); $destfile{$x} = 1; } @$possibles;

    my @ret;
    foreach my $possible (@$possibles) {
	my ($prefix, $srcpath) = @$possible;
	my $destpath = $this->transform($srcpath);
	next if defined $destfile{$srcpath}; # skip sources which are the destination for something else
	if (! -f $destpath ||
	    $this->destfile_out_of_date($prefix, $srcpath, $destpath)) {
	    push(@ret, [ $prefix, $srcpath, $destpath ]);
	}
    }
    return @ret;
}

sub file_is_source {
    my($this,$prefix,$fullpath,$filename) = @_;

    return 1 if $fullpath =~ /\.ds$/o;
    return 0;
}

sub destination_file {
    my($this,$prefix,$fullpath) = @_;

    return $this->transform($fullpath);
}

sub rebuild {
    my($this,$prefix,$srcpath,$destpath) = @_;

    my $destdir = $destpath;
    $destdir =~ s!/[^/]+$!!o;
    eval { mkpath($destdir); };
    die "Unable to create $destdir: $@" 
	unless -d $destdir;
    # dsrepack won't overwrite files even though we are writing $file-new
    if ($this->{target_file_size} eq '') {
	if (-f $destpath) {
	    unlink($destpath) or die "can't unlinnk $destpath: $!";
	}
	my @cmd = grep(defined, "dsrepack", @{$this->{compress}}, $this->{extent_size}, $srcpath, $destpath);
	print join(" ", @cmd), "\n";
	return system(@cmd) == 0;
    } else {
	my $basename = $destpath;
	$basename =~ s/\.ds-new//o 
	    || die "Can't strip .ds-new from end of $destpath??";
	for(my $i = 0; 1; ++$i) {
	    my $splitname = sprintf("%s.part-%02d.ds", $basename, $i);
	    last unless -f $splitname;
	    unlink($splitname) or die "can't unlink $splitname: $!";
	}
	my @cmd = grep(defined, "dsrepack", @{$this->{compress}}, $this->{extent_size}, $this->{target_file_size}, $srcpath, $basename);
	print join(" ", @cmd), "\n";
	my $ret = system(@cmd) == 0;
	if ($ret) {
	    # Mark (with empty file) we are done
	    my $fh = new FileHandle ">$destpath"
		or die "Can't open $destpath for write: $!";
	    close($fh) 
		or die "Close failed: $!";
	}
	return $ret;
    }
    die "??";
}

1;

