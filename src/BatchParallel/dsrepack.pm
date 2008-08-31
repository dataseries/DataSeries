#
#  (c) Copyright 2006-2007, Hewlett-Packard Development Company, LP
#
#  See the file named COPYING for license details
#

package BatchParallel::dsrepack;
use strict;
use warnings;
use FileHandle;
use vars '@ISA';
@ISA = qw/BatchParallel::common/;

$| = 1;
# TODO: support multiple transforms, right now you can only transform
# merges back into the same directory they came from.

sub new {
    my $class = shift;

    my $this = bless {
	'compress' => [],
	'verbose_level' => 1,
	'ignore_missing_files' => 0,
    }, $class;
    while (@_ > 0) {
	$_ = shift @_;
	if ($_ eq 'help') {
	    $this->usage();
	    exit(0);
# TODO: pull the following two bits out into a BatchParallel::dscommon 
# module; re-use it in the various dataseries modules
	} elsif (/^compress=((lzo)|(gz)|(bz2)|(lzf))$/o) {
	    push(@{$this->{compress}}, "--enable-$1");
	} elsif (/^compress-level=([1-9])$/o) {
	    $this->{compress_level} = "--compress-level=$1";
	} elsif (/^extent-size=(\d+(\.\d+)?)([km])?$/o) {
	    die "Can't set multiple extent sizes" 
		if defined $this->{extent_size};
	    my ($size,$units) = ($1,$3);
	    $units ||= '';
	    $size *= 1024 if $units eq 'k';
	    $size *= 1024 * 1024 if $units eq 'm';
	    $size = sprintf("%d", $size);
	    $this->{extent_size} = "--extent-size=$size";
	} elsif (/^mode=split:(\d+)([kmg])$/o) {
	    die "Can't set multiple modes" if defined $this->{mode};
	    $this->{mode} = 'simo'; # single in multiple out
	    my ($size,$units) = ($1,$2);
	    $units ||= '';
	    $size *= 1024 if $units eq 'k';
	    $size *= 1024 * 1024 if $units eq 'm';
	    $size *= 1024 * 1024 * 1024 if $units eq 'g';
	    $size = sprintf("%.10g", $size / (1024*1024));
	    $this->{target_file_size} = "--target-file-size=$size";
	} elsif (/^mode=merge$/o) {
	    die "Can't set multiple modes" if defined $this->{mode};
	    $this->{mode} = 'miso'; # multiple in single out
	    die "already set the transform or require options"
		if defined $this->{transform} || defined $this->{require};

	    $this->{merge_re} = '/\.part-(\d+)\.ds$/o';
	    $this->{require_re} = '/\.part-\d+\.ds$/o'; 
	    $this->{transform} = 's/\.part-\d+\.ds$/.ds/o';
	} elsif (/^mode=merge:(\d+):(.+)$/o) {
	    die "Can't set multiple modes" if defined $this->{mode};
	    $this->{mode} = 'miso';
	    die "already set the transform or require options"
		if defined $this->{transform} || defined $this->{require};

	    $this->{merge_count} = $1;
	    die "merge count not > 1" unless $this->{merge_count} > 1;
	    $this->{merge_re} = $2;
	    $this->{require} = $this->{merge_re};
	} elsif (/^transform=(.+)$/o) {
	    die "Can't set multiple transforms" 
		if defined $this->{transform};
	    $this->{transform} = $1;
	} elsif (/^require=(.+)$/o) {
	    die "Can't set multiple requires" 
		if defined $this->{require_re};
	    $this->{require_re} = $1;
	} elsif (/^ignore=(.+)$/o)  {
	    die "Can't set multiple ignoress" 
		if defined $this->{ignore_re};
	    $this->{ignore_re} = $1;
	} elsif (/^ignore-missing-files$/o) {
	    $this->{ignore_missing_files} = 1;
	} else {
	    die "unknown options specified for batch-parallel module $class: '$_'";
	}
    }
    $this->{mode} ||= 'siso'; # single in single out

    unless (defined $this->{transform}) {
	$this->{transform} = 's!^(.*)\.ds$!$1-repack.ds!o'
	    if $this->{mode} eq 'siso';
	$this->{transform} = '$_ .= "-split-done"'
	    if $this->{mode} eq 'simo';
	die "missing transform?" unless defined $this->{transform};
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

# TODO: move this into Pod::Usage or something
sub usage {
    print <<'END_OF_USAGE';
batch-parallel dsrepack [compress={bz2,lzf,gz,lzo}] [compress-level=0-9]
  [extent-size=#[km]] [mode=split:#kmg] [mode=merge[:#:perl-expr]]
  [transform={perl-expr}|subdir] [ignore-missing-files]
  [ignore=regex] [require=regex]
  -- file/directory...

  The compress option can be specified multiple times to select multiple
  compression algorithms.

  The ignore and require regexs allow selection of files either not
  matching (ignore) or matching (require) the respective regex.

  The dsrepack module has three modes:

  1) 1 file to 1 file transforms; this is the default mode, the
     default transform is to add -repack to the name before the .ds.
     This mode will ignore any files which are destination files of
     any of the input files.

  2) 1 file to many file transforms; this is the split mode. It sets
     the ignore regex to /\.part-##.ds\$/, and sets the default
     transform to add -split-done to the input name.  This mode will
     create an empty output file of the destination transform so that
     we can tell if the split has been run and is up to date.  Hence
     it is an error for the destination file to exist of non-zero
     size.

  3) many files to one file transforms; this is the merge mode.  
     In this mode the transform has @_ set to the list of files in
     each merge group.  Both of these will only combine files that map 
     to the same output under the transform.
     a) In mode=merge, this sets the require regex to be /\.part-##.ds\$/, 
        and the transform removes the .part-## portion of the name
     b) In mode=merge:#:regex, this sets the require regex to regex,
        extracts $1 from each regex. Then it sorts the files by that value
        either numerically or alphabetically. Finally the transform is set to
        create files named <transform-output>.$first-$last.ds where
        $first and $last are the first and last $1's of each group.
     If there are any missing files in the list of numbers, a warning will
     be printed.  This can be skipped with the ignore-missing-files option.

Examples:

 # Take files named *.###{,.prune}.ds, make groups of 101 files
 # (eliminated a group of 1 file and a group of 6); rewrite the paths
 # to put them in an entirely different place, and make them all named 
 # network.#-#.ds (This is how we prepared the NFS traces for distribution)
 % batch-parallel --noshuffle -n dsrepack mode=merge:101:'/\.(\d+)(\.prune)?\.ds$/o' compress=bz2 extent-size=16m transform='s,/mnt/fileserver-2/a/(.+)/[^/]+\.\d+(\.prune)?\.ds,/mnt/fileserver-1/b/user/anderse/bz2-pack/$1/network,o' -- set-?

END_OF_USAGE

# Example complex merge
# 

}

sub transform {
    my($this, $inname) = @_;

    local $_;
    $_ = $inname;
    eval $this->{transform};
    die "Transforming $inname failed: $@\nTransform was: $this->{transform}\n " 
	if $@;
    die "Transforming $inname did not change it.\nTransform was $this->{transform}\n "
	if $_ eq $inname;
    return $_;
}

sub find_things_to_build {
    my($this, @dirs) = @_;

    my @possibles = $this->find_possibles(@dirs);

    if (defined $this->{require_re}) {
	my $re = qr/$this->{require_re}/o;
	@possibles = grep($_->[1] =~ $re, @possibles);
    }
    @possibles = grep($_->[1] !~ /$this->{ignore_re}/o, @possibles)
	if defined $this->{ignore_re};

    # Ignore any file which is a destination file of another file we're processing.
    my %dest2sources;

    for (@possibles) {
	my $out = $this->transform($_->[1]);
	die "both $dest2sources{$out} and $_->[1] map to the same output"
	    if defined $dest2sources{$out} && $this->{mode} ne 'miso';
	push(@{$dest2sources{$out}}, $_->[1]);
    }

    my $source_count = 0;
    my @ret;

    if ($this->{mode} eq 'siso' || $this->{mode} eq 'simo') {
	for (@possibles) {
	    my $srcpath = $_->[1];
	    # skip sources which are the destination for something else
	    next if defined $dest2sources{$srcpath}; 

	    ++$source_count;
	    my $destpath = $this->transform($srcpath);
	    if ($this->file_older($destpath, $srcpath)) {
		if ($this->{mode} eq 'siso') {
		    push(@ret, DSRepackSISO->new($this,$srcpath,$destpath));
		} else {
		    push(@ret, DSRepackSIMO->new($this,$srcpath,$destpath));
		}
	    }
	}
    } elsif ($this->{mode} eq 'miso') {
	foreach my $destpath (sort keys %dest2sources) {
	    my $sources = $dest2sources{$destpath};
	    die "Only found 1 source ($sources->[0]) for $destpath?"
		unless @$sources > 1;
	    my %source2order;
	    my $all_nums = 1;
	    foreach my $src (@$sources) {
		$_ = $src;
		my $order = eval "$this->{merge_re}; return \$1;";
		die "error on eval of '$this->{merge_re}; return $1;': $@"
		    if $@;
		die "didn't get \$1 set from $this->{merge_re} on $_"
		    unless defined $order && length $order > 0;
		$source2order{$src} = $order;
		$all_nums = 0 unless $order =~ /^\d+$/o;
	    }
	    my @sources;
	    if ($all_nums) {
		@sources = sort { $source2order{$a} <=> $source2order{$b} } @$sources;
		my $prev = $source2order{$sources[0]} - 1;
		for my $src (@sources) {
		    ++$prev;
		    if ($prev != $source2order{$src}) {
			print "Warning, expected to find file $prev, instead found $src, file $source2order{$src}.\n";
			print "Continue [y]? ";
			if ($this->{ignore_missing_files}) {
			    print " [yes, ignoring warning]\n";
			} else {
			    $_ = <STDIN>;
			    exit(1) unless /^$/o || /^y/io;
			}
			$prev = $source2order{$src};
		    }
		}
	    } else {
		@sources = sort { $source2order{$a} cmp $source2order{$b} } @$sources;
	    }		
		
	    if (defined $this->{merge_count}) {
		my $destbase = $destpath;
		warn "$destpath ends with .ds, this probably isn't what you want."
		    if $destbase =~ /\.ds$/o;
		while (@sources > 0) {
		    my @chunk = splice(@sources, 0, $this->{merge_count});
		    my $start = $source2order{$chunk[0]};
		    my $end = $source2order{$chunk[@chunk - 1]};
		    my $chunkdest = "${destbase}.${start}-${end}.ds";
		    ++$source_count;
		    if ($this->file_older($chunkdest, @chunk)) {
			push(@ret, DSRepackMISO->new($this, \@chunk, $chunkdest));
		    }
		}
	    } else {	
		++$source_count;
		if ($this->file_older($destpath, @$sources)) {
		    push(@ret, DSRepackMISO->new($this, \@sources, $destpath));
		}
	    }
	}
    }
    return ($source_count, @ret);
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

sub rebuild_thing_do {
    my($this,$thing) = @_;

    $thing->rebuild_thing_do();
}

sub rebuild_thing_success {
    my($this,$thing) = @_;

    $thing->rebuild_thing_success();
}

sub rebuild_thing_fail {
    my($this,$thing) = @_;

    $thing->rebuild_thing_fail();
}

sub rebuild_thing_message {
    my($this,$thing) = @_;

    $thing->rebuild_thing_message();
}
    
sub getResourceUsage {
    my($this, $scheduler) = @_;

    # Assume we will consume 100% of the cpu through parallel repack
    return "rusage[ut=1]" if $scheduler eq 'lsf';
    warn "Do not know how to specify resource utilization for scheduler '$scheduler'";
    return '';
}

package DSRepackThing;

use File::Path;

sub new {
    my($class, $base, $src, $dest) = @_;

    die "??" unless defined $dest;
    return bless { 'base' => $base,
		   'src' => $src,
		   'dest' => $dest }, $class;
}

sub setup_dest_dir {
    my($this, $destpath) = @_;

    my $destdir = $this->{dest};
    die "??" unless defined $destdir;
    $destdir =~ s!/[^/]+$!!o;
    eval { mkpath($destdir); };
    die "Unable to create $destdir: $@" 
	unless -d $destdir;
}    

sub rebuild_thing_do {
    my($this) = @_;

    my $old_modify = -M "$this->{dest}-new" || 1;

    unless ($this->rebuild("$this->{dest}-new")) {
	die "rebuild of $this->{dest} failed";
    }

    my $new_modify = -M "$this->{dest}-new";
    die "?? $old_modify $new_modify" 
	unless -e "$this->{dest}-new" && $old_modify > $new_modify;
    exit(0);
}

sub rebuild_thing_success {
    my($this) = @_;

    die "?? $this->{dest}-new" unless -e "$this->{dest}-new";
    die "??2 - $this->{dest}-new - " . (-M "$this->{dest}-new") 
	unless -M "$this->{dest}-new" <= 1; # Could be 1 second in future because filesystem may only store seconds.
    rename("$this->{dest}-new", $this->{dest})
	or die "Can't rename $this->{dest}-new to $this->{dest}: $!";
}

sub rebuild_thing_fail {
    my($this) = @_;

    print "Rebuilding $this->{dest}-new from $this->{src} failed: $!";
}

package DSRepackSISO; # single in single out

use strict;
use vars '@ISA';

@ISA = qw(DSRepackThing);

sub rebuild {
    my($this, $dest) = @_;

    $this->setup_dest_dir($dest);

    # dsrepack won't overwrite files even though we are writing $file-new
    if (-f $dest) {
	unlink($dest) or die "can't unlink $dest: $!";
    }

    my $base = $this->{base};
    
    $base->run(grep(defined, "dsrepack", @{$base->{compress}}, 
		    $base->{compress_level}, $base->{extent_size}, 
		    $this->{src}, $dest));
    return 1;
};

sub rebuild_thing_message {
    my($this) = @_;

    print "Should rebuild $this->{dest} from $this->{src}\n";
}

package DSRepackSIMO; # single in multiple out

use strict;
use File::Path;
use vars '@ISA';

@ISA = qw(DSRepackThing);

sub basename {
    my($this, $dest) = @_;

    my $basename = $dest;
    $basename =~ s/\.ds-new//o 
	|| die "Can't strip .ds-new from end of $dest??";

    return $basename;
}

sub rebuild {
    my($this, $dest) = @_;

    $this->setup_dest_dir($dest);

    my $basename = $this->basename($dest);
    for(my $i = 0; 1; ++$i) {
	my $splitname = sprintf("%s.part-%02d.ds", $basename, $i);
	last unless -f $splitname;
	unlink($splitname) or die "can't unlink $splitname: $!";
    }

    my $base = $this->{base};
    
    $base->run(grep(defined, "dsrepack", @{$base->{compress}}, 
		    $base->{compress_level}, $base->{extent_size}, 
		    $base->{target_file_size}, $this->{src}, $basename));
    
    # Mark (with empty file) we are done
    my $fh = new FileHandle ">$dest"
	or die "Can't open $dest for write: $!";
    close($fh) 
	or die "Close failed: $!";

    return 1;
}

sub rebuild_thing_message {
    my($this) = @_;

    my $basename = $this->basename($this->{dest});

    print "Should rebuild ${basename}.part-\#\#.ds from $this->{src}\n";
}

package DSRepackMISO; # multiple in, single out

use strict;
use File::Path;
use vars '@ISA';

@ISA = qw(DSRepackThing);

sub rebuild {
    my($this, $dest) = @_;

    $this->setup_dest_dir($dest);

    my $base = $this->{base};
    
    # dsrepack won't overwrite files even though we are writing $file-new
    if (-f $dest) {
	unlink($dest) or die "can't unlink $dest: $!";
    }
    $base->run(grep(defined, "dsrepack", @{$base->{compress}}, 
		    $base->{compress_level}, $base->{extent_size}, 
		    @{$this->{src}}, $dest));
    return 1;
}

sub rebuild_thing_message {
    my($this) = @_;

#    my $sources = join(", ", @{$this->{src}});
    my $nsources = @{$this->{src}};
    my $sources = "$this->{src}->[0] .. $this->{src}->[$nsources-1]";
    print "Should rebuild $this->{dest} from $sources\n";
}
     
1;

