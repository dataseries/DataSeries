#
#  (c) Copyright 2005, Hewlett-Packard Development Company, LP
#
#  See the file named COPYING for license details
#

package BatchParallel::nettrace2ds;

@ISA = qw/BatchParallel::common/;

sub new {
    my $class = shift;

    my $this = bless {}, $class;
    while (@_ > 0) {
	$_ = shift @_;
	if ($_ eq 'help') {
	    $this->usage();
	    exit(0);
	} elsif (/^info$/o) {
	    die "already have a mode" if defined $this->{mode};
	    $this->{mode} = 'info';
	} elsif (/^infodir=(\S+)$/o) {
	    $this->{infodir} = $1;
	} elsif (/^groupsize=(\d+)$/o) {
	    $this->{groupsize} = $1;
	} elsif (/^compress=(.+)$/o) {
	    die "Already have a filter" if defined $this->{compress};
	    $this->{compress} = $1;
	} else {
	    die "unknown options specified for batch-parallel module $class: '@_'";
	}
    }
    die "Need to define infodir" unless defined $this->{infodir};
    die "$this->{infodir} is not a directory" unless -d $this->{infodir};
    return $this;
}

sub usage {
    print "batch-parallel nettrace2ds look at the code\n";
}

sub file_is_source {
    my($this,$prefix,$fullpath,$filename) = @_;

    return 1 if $filename =~ /^endace\.\d+\.128MiB\.((lzf)|(zlib\d))$/o;
    return 0;
}

sub find_things_to_build {
    my($this, @sources) = @_;

    my ($source_count, @file_list) = $this->SUPER::find_things_to_build(@sources);
    
    my %num_to_file;
    map { /endace\.(\d+)\.128MiB.\w+$/o || die "huh $_"; 
	  die "Duplicate number $1 from $_ and $num_to_file{$1}"
	      if defined $num_to_file{$1};
	  $num_to_file{$1} = $_; } @file_list;

    $this->{groupsize} = int(sqrt($source_count)) unless defined $this->{groupsize};
    
    my @nums = sort { $a <=> $b } keys %num_to_file;
    
    my @ret;
    my $group_count = 0;
    for(my $i=0; $i < @nums; $i += $this->{groupsize}) {
	my @group;
	my $first_num = $nums[$i];
	my $last_num = $nums[$i+$this->{groupsize}-1] || $nums[@nums-1];
	for(my $j = $first_num; $j <= $last_num; ++$j) {
	    my $k = sprintf("%06d", $j);
	    die "missing num $k" unless defined $num_to_file{$k};
	    push(@group, $num_to_file{$k});
	    delete $num_to_file{$k};
	}
	++$group_count;
	my $infoname = "$this->{infodir}/info.$first_num-$last_num";
	push(@ret, { 'first' => $first_num, 'last' => $last_num, 'files' => \@group,
		     'infoname' => $infoname })
	    if ! -f $infoname || $this->file_older($infoname, @group);
    }
    return ($group_count, @ret);
}

sub determine_things_to_build {
    return map { $_->[1] } @{$_[1]};
}

sub rebuild_thing_do {
    my($this, $thing_info) = @_;

    my $cmd = "nettrace2ds --info-erf " . join(" ", @{$thing_info->{files}}) . " >$thing_info->{infoname}-new";
    print "$cmd\n";
    my $ret = system($cmd);
    exit(1) unless $ret == 0;
    exit(0);
}

sub rebuild_thing_success {
    my($this, $thing_info) = @_;

    die "huh" unless -f "$thing_info->{infoname}-new";
    rename("$thing_info->{infoname}-new",$thing_info->{infoname})
	or die "rename failed: $!";
}

sub rebuild_thing_fail {
    die "unimplemented";
}

sub rebuild_thing_message {
    die "unimplemented";
}

1;

