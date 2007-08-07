package BatchParallel::bacct2ds;
use BatchParallel::nonmod::gpgkeys;
use File::Path;

die "module version mismatch" 
    unless $BatchParallel::common::interface_version < 2;

@ISA = qw/BatchParallel::common/;

sub usage {
    print <<END_OF_USAGE;
batch-parallel bacct2ds [help]
  [key=<nokey, key-filename, or ~/.<key>-hash-keys.txt.gpg>; 
   default autoguess from source files] 
  [bacct2ds=<binary-path, by default found in \$PATH] 
  [no-binary-dependency]
  [compress=<bz2,gz,lzo,lzf, as supported by bacct2ds, default lzo>] 
  -- <source files>
END_OF_USAGE
}

sub new {
    my $class = shift;
    my $this = { 'keyguess' => {}, keyfile => undef, 
		 'bacct2ds' => undef,
		 'compress' => 'lzo' };
    bless $this, $class;
    my $bindep = 1;
    foreach my $arg (@_) {
	if ($arg eq 'help') {
	    $this->usage();
	    exit(0);
	} elsif ($arg =~ /^key=(.+)$/o) {
	    $this->{keyfile} = $1;
	} elsif ($arg =~ /^bacct2ds=(.+)$/o) {
	    $this->{bacct2ds} = $1;
	    die "$this->{bacct2ds} isn't executable"
		unless -x $this->{bacct2ds};
	} elsif ($arg =~ /^compress=(\w+)$/o) {
	    $this->{compress} = $1;
	} elsif ($arg eq 'no-binary-dependency') {
	    $bindep = 0;
	} else {
	    $this->usage();
	    die "don't understand argument '$arg'";
	}
    }
    unless(defined $this->{bacct2ds}) {
	my @dirs = split(/:/o,$ENV{PATH});
	foreach my $dir (@dirs) {
	    if (-x "$dir/bacct2ds") {
		$this->{bacct2ds} = "$dir/bacct2ds";
		last;
	    }
	}
	die "couldn't find bacct2ds in \$PATH"
	    unless defined $this->{bacct2ds};
	print "Found bacct2ds as $this->{bacct2ds}\n";
    }
    push(@{$this->{static_deps}},$this->{bacct2ds})
	if $bindep;

    return $this;
}

sub file_is_source {
    my($this,$prefix,$fullpath,$filename) = @_;

    $this->{keyguess}->{tiger} = 1 if $fullpath =~ m!Tuscany/Tiger!o;
    $this->{keyguess}->{bear} = 1 if $fullpath =~ m!Grizzly/Bear!o;
    $this->{keyguess}->{bear} = 1 if $fullpath =~ m!lsf-data/rwc/!o;
    $this->{keyguess}->{bear} = 1 if $fullpath =~ m!lsf-data/gld/!o;

    return 0 if $fullpath =~ /\.ds(-new)?$/o;
    return 0 if $fullpath =~ /\~$/o;
    return 1 if $fullpath =~ /\blsb\.acct\b/o;
    return 0;
}

sub pre_exec_setup {
    my($this) = @_;

    return if defined $ENV{'NAME_KEY_1'} && defined $ENV{'NAME_KEY_2'};
    unless (defined $this->{keyfile}) {
	my @guesses = keys %{$this->{keyguess}};
	if (scalar @guesses == 1) {
	    $this->{keyfile} = "$ENV{HOME}/.$guesses[0]-hash-keys.txt.gpg";
	} elsif (scalar @guesses == 0) {
	    die "unable to guess key file name from input files";
	} else {
	    die "multiple guesses of key file name (" . join(", ",@guesses) . ") from input files";
	}
    }
    BatchParallel::nonmod::gpgkeys::setup_keys($this->{keyfile});
}

sub destination_file {
    my($this,$prefix,$fullpath) = @_;

    if ($fullpath =~ m!.*/lsf-data/(\w+)/timesplit/!o) {
	$fullpath =~ s/\.((gz)|(bz2))$//o;
	$fullpath =~ s!/timesplit/!/ds/!o;

	my $dirpath = $fullpath;
	$dirpath =~ s![^/]+!!o;
	mkpath($dirpath) unless -d $dirpath;
	return $fullpath . ".ds";
    } else {
	die "?? $fullpath";
    }
}

sub destfile_out_of_date {
    my($this,$prefix,$fullpath,$destfile) = @_;

    return $this->file_older($destfile,$fullpath,@{$this->{static_deps}});
}
    

sub rebuild {
    my($this,$prefix,$fullpath,$destpath) = @_;

    my $command;

    if ($fullpath =~ /\.gz$/o) {
	$command = "gunzip -c < $fullpath | ";
    } elsif ($fullpath =~ /\.bz2$/o) {
	$command = "bunzip2 -c < $fullpath | ";
    } else {
	$command = "cat $fullpath | ";
    }

    my $cluster;
    if ($fullpath =~ m!/lsf-data/(\w+)/timesplit!o) {
	$cluster = $1;
    } else {
	die "?? can't work out cluster from $fullpath";
    }

    $command .= "$this->{bacct2ds} --compress-$this->{compress} - $cluster $destpath";
    print "$command\n";

    return system($command) == 0;
}
    
1;
