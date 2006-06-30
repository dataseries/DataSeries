#
#  (c) Copyright 2005, Hewlett-Packard Development Company, LP
#
#  See the file named COPYING for license details
#

package BatchParallel::ds2txt;

@ISA = qw/BatchParallel::common/;

sub new {
    my $class = shift;

    my $this = bless {}, $class;
    while (@_ > 0) {
	$_ = shift @_;
	if ($_ eq 'help') {
	    $this->usage();
	    exit(0);
	} elsif (/^filter=(.+)$/o) {
	    die "Already have a filter" if defined $this->{filter};
	    $this->{filter} = $1;
	} elsif (/^output_extension=(\w+)$/o) {
	    die "Already have an output extension" if defined $this->{output_extension};
	    $this->{output_extension} = $1;
	} else {
	    die "unknown options specified for batch-parallel module $class: '@_'";
	}
    }
    return $this;
}

sub usage {
    print "batch-parallel ds2txt [filter=<command>] -- file/directory...\n";
}

sub file_is_source {
    my($this,$prefix,$fullpath,$filename) = @_;

    return 1 if $fullpath =~ /\.ds$/o;
    return 0;
}

sub destination_file {
    my($this,$prefix,$fullpath) = @_;

    $fullpath =~ s/\.ds/.txt/o;
    $fullpath .= ".$this->{output_extension}"
        if defined $this->{output_extension};
    return $fullpath;
}

sub rebuild {
    my($this,$prefix,$fullpath,$destpath) = @_;

    my $command = "ds2txt $fullpath";
    $command .= " | $this->{filter}" if defined $this->{filter};
    $command .= " >$destpath";
    print "$command\n";
    return system($command) == 0;
}

1;

