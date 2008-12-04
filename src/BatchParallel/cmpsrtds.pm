#
#  (c) Copyright 2005, Hewlett-Packard Development Company, LP
#
#  See the file named COPYING for license details
#

package BatchParallel::cmpsrtds;

@ISA = qw/BatchParallel::common/;

sub new {
    my $class = shift;

    my $this = bless {}, $class;
    while (@_ > 0) {
	$_ = shift @_;
	if ($_ eq 'help') {
	    $this->usage();
	    exit(0);
	} elsif (/^minor=(.+)$/o) {
	    die "Already have a new minor version" if defined $this->{new_minor};
	    $this->{new_minor} = $1;
	} else {
	    die "unknown options specified for batch-parallel module $class: '@_'";
	}
    }
    return $this;
}

sub usage {
    print "batch-parallel cmpsrtds -- file/directory...\n";
}

sub file_is_source {
    my($this,$prefix,$fullpath,$filename) = @_;
    if ($fullpath =~ /\.srt(|(\.Z)|(\.bz2)|(\.gz))$/o) {
	print "$fullpath matches\n";
	return 1;
    }
    return 0;
}

sub destination_file {
    my($this,$prefix,$fullpath) = @_;
    $fullpath =~ s/\.srt(|(\.Z)|(\.bz2)|(\.gz))$/.out/o;
    print "destfullpath $fullpath\n";
    return $fullpath;
}

sub rebuild {
    my($this,$prefix,$fullpath,$destpath) = @_;
    my $new_minor = '';
    if (defined ($this->{new_minor})) {
	$new_minor = $this->{new_minor};
    }
    $dsfilepath = $fullpath;
    $dsfilepath =~ s/\.srt(|(\.Z)|(\.bz2)|(\.gz))$/.ds/o;
    
    my $command = "cmpsrtds $fullpath $dsfilepath $new_minor &> $destpath";
    print "$command\n";
    return system($command) == 0;
}

1;

