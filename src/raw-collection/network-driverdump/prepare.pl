#!/usr/bin/perl -w
use strict;
use English;

usage() unless @ARGV >= 1;
usage("You need to be root") unless 0 == $EUID;

$|=1;

if ($ARGV[0] eq 'partition') {
    partition_drives();
} elsif ($ARGV[0] eq 'add_serial') {
    shift @ARGV;
    add_serial(@ARGV);
} elsif ($ARGV[0] eq 'format') {
    format_drives();
} elsif ($ARGV[0] eq 'mount') {
    mount_drives();
} elsif ($ARGV[0] eq 'umount') {
    umount_drives();
} else {
    usage("Unknown command '$ARGV[0]'");
}

sub bydriveorder {
    return -1 if length $a < length $b; 
    return 1 if length $a > length $b; 
    return $a cmp $b;
} 


sub find_drives {
    return @Global::drives if @Global::drives > 0;
    open(CONFIG,"/etc/driverdump.config") 
	or die "Unable to open /etc/driverdump.config for read: $!";
    print "Detecting drives for tracing ";
    my @drivepatterns;
    my %serials;
    while(<CONFIG>) {
	next if /^#/o || /^\s*$/o;
	if (/^drivepattern (\S+)$/o) {
	    push(@drivepatterns, $1);
	} elsif (/^serial (\S+)$/o) {
	    $serials{$1} = 1;
	} else {
	    chomp;
	    die "Unrecognized line in /etc/driverdump.config: '$_'";
	}
    }

    opendir(DEV, "/dev") or die "bad";
    my @drives;
    while($_ = readdir(DEV)) {
	foreach my $pat (@drivepatterns) {
	    if (/^$pat$/) {
		push(@drives, "$_");
		last;
	    }
	}
    }
    closedir(DEV);
    @drives = sort bydriveorder @drives;
    my $count = @drives;
    print "($count possibles): ";
    my @nosginfo;
    foreach my $drive (@drives) {
	is_drive($drive);
	my $serial = serial_number($drive);
	if (! defined $serial) {
	    push(@nosginfo, $drive);
	} elsif ($serials{$serial}) {
	    print ", " if @Global::drives;
	    push(@Global::drives, $drive);
	    print "$drive";
	}
    }
    print "\n";
    die "Unable to find any drives for tracing" 
	unless @Global::drives;
    if (@nosginfo) {
	print "Unable to get sginfo -s on: ", join(", ", @nosginfo), "\n";
    }
    return @Global::drives;
}

sub add_serial {
    my(@drives) = @_;

    foreach my $drive (@drives) {
	is_drive($drive);
	my $serial = serial_number($drive);
	open(CONFIG, ">>/etc/driverdump.config");
	print CONFIG "serial $serial\n";
    }
}
	

sub partition_drives {
    my @drives = find_drives();

    my %ok = get_ok_partitions(@drives);

    print "drives partitioned correctly: ";
    my @ok = sort bydriveorder keys %ok;
    print join(", ", @ok), "\n";
    print "partitioning: ";
    my $first = 1;
    foreach my $drive (@drives) {
	next if $ok{$drive};
	print ", " unless $first;
	$first = 0;
	print "$drive";
	open(SFDISK, "| sfdisk /dev/$drive >/dev/null 2>&1")
	    or die "??";
	print SFDISK "0,\n";
	close(SFDISK);
	die "??" unless $? == 0;
    }
    print "\n";
    if ($first == 0) {
	print "checking...";
	my %new_ok = get_ok_partitions(@drives);
	foreach my $drive (@drives) {
	    die "$drive isn't ok??" unless $new_ok{$drive};
	}
	print "all correct.\n";
    }
}

sub format_drives {
    my @drives = find_drives();
    my %ok = get_ok_partitions(@drives);
    my %mounted = get_mounted();

    print "formatting (in parallel): ";
    my $ok = 1;
    my %children;
    foreach my $drive (@drives) {
	print ", " unless $drive eq $drives[0]; 
	print "$drive";
	unless ($ok{$drive}) {
	    warn "\n$drive isn't partitioned correctly, aborting...";
	    $ok = 0;
	    last;
	}
	if ($mounted{$drive}) {
	    warn "\n$drive is already mounted, aborting...";
	    $ok = 0;
	    last;
	}
	my $pid = fork();
	unless (defined $pid && $pid >= 0) {
	    warn "\nfork failed: $!";
	    $ok = 0;
	    last;
	}
	if ($pid == 0) {
	    my $ret = system("dd if=/dev/zero of=/dev/${drive}1 bs=1024k count=1 >/dev/null 2>&1");
	    die "\ndd failed for /dev/${drive}1" 
		unless $ret == 0;
	    my $fs = 'xfs';
	    if ($fs eq 'ext2') {
		$ret = system("mkfs.ext2 -m 0 -O sparse_super -T largefile4 /dev/${drive}1 >/dev/null 2>&1");
		die "\nmkfs.ext2 failed for /dev/${drive}1"
		    unless $ret == 0;
	    } elsif ($fs eq 'xfs')  {
		$ret = system("mkfs.xfs /dev/${drive}1 >/dev/null 2>&1");
		die "\nmkfs.xfs failed for /dev/${drive}1"
		    unless $ret == 0;
	    } else {
		die "huh $fs";
	    }
	    exit(0);
	}
	$children{$pid} = $drive;
	select(undef,undef,undef,0.1);
    }
    print "\n";
    print "waiting: ";
    my $first = 1;
    my %success;
    while((my $pid = wait) > 0) {
	print ", " unless $first;
	$first = 0;
	$children{$pid} ||= "UNKNOWN";
	print "$children{$pid}";
	$success{$children{$pid}} = 1;
	delete $children{$pid};
    }
    print "\n";
    foreach my $drive (@drives) {
	die "Failed to format $drive" unless $success{$drive};
    }
}
    
sub mount_drives {
    my @drives = find_drives();
    my %mounted = get_mounted();
    
    unless(-d "/mnt") {
	mkdir("/mnt",0755) or die "bad";
    }

    print "Cleaning...";
    my $first = 1;
    opendir(DIR,"/mnt") or die "bad";
    while(my $dir = readdir(DIR)) {
	next unless $dir =~ /^trace-(\S+)$/o;
	next if $mounted{$1};

	print ", " unless $first;
	$first = 0;
	print "$dir";
	rmdir("/mnt/$dir") or die "bad /mnt/$dir";
    }
    closedir(DIR);
    print "; " unless $first;
    print "done.\n";

    $first = 1;
    print "Mounting...";
    foreach my $drive (@drives) {
	next if $mounted{$drive};
	print ", " unless $first; 
	$first = 0;
	print "$drive";
	mkdir("/mnt/trace-$drive",0755) or die "bad: $!";
	my $ret = system("mount /dev/${drive}1 /mnt/trace-$drive");
	die "mount failed" unless $ret == 0;
    }
    print "; " unless $first;
    print "done.\n";
}

sub umount_drives {
    my %mounted = get_mounted();

    print "Unmounting...";
    my $first = 1;
    foreach my $mounted (sort bydriveorder keys %mounted) {
	print ", " unless $first;
	$first = 0;
	print "$mounted";
	my $ret = system("umount /mnt/trace-$mounted");
	die "umount failed" unless $ret == 0;
	rmdir("/mnt/trace-$mounted") or die "bad";
    }
    print "; " unless $first;
    print "done.\n";
}    

sub get_mounted {
    my %mounted;
    open(MOUNT, "mount |") or die "??";
    while(<MOUNT>) {
	next unless m!^/dev/(\S+)1 on /mnt/trace-(\S+) !o;
	die "?? $1 $2" unless $1 eq $2;
	$mounted{$2} = 1;
    }
    close(MOUNT);
    return %mounted;
}


sub get_ok_partitions {
    my @drives = @_;
    my %ok;
    open(SFDISK, "sfdisk -uS -l 2>&1 |") or die "bad";
    while(<SFDISK>) {
	die "??1" unless /^\s*$/o;
	$_ = <SFDISK>;
	die "?? $_ ??" unless m!^Disk /dev/(\S+): (\d+) cylinders, (\d+) heads, (\d+) sectors/track!o;
	my ($drive, $cylinders, $sectorspercyl) = ($1,$2, $3*$4);
	$_ = <SFDISK>;
	if (/^Warning: The partition table/o) {
	    $_ = <SFDISK>; die "??" unless m!^  for C/H/S=!o;
	    $_ = <SFDISK>; die "??" unless m!^For this listing I'll assume!o;
	    $_ = <SFDISK>;
	}
	if (/^$/o) {
	    $_ = <SFDISK>;
	    die "??" unless /^sfdisk: ERROR: sector 0 does not have an msdos signature/o;
	    $_ = <SFDISK>;
	    die "??" unless m!^ /dev/${drive}: unrecognized partition table type!;
	    $_ = <SFDISK>;
	    die "??" unless /^No partitions found/o;
	    next;
	}
	die "$_ ?? " unless m!^Units = sectors of 512 bytes, counting from 0$!o;
	$_ = <SFDISK>;
	die "??" unless /^\s*$/o;
	$_ = <SFDISK>;
	die "$_ ??" unless /^   Device Boot    Start       End   #sectors  Id  System$/o;
	$_ = <SFDISK>;
	die "?? $drive\n$_ ??" unless m!^/dev/${drive}p?1\s+!;
	my @bits = split(/\s+/o);

	# Since we will not have DOS access these disks and we don't
	# boot from them, don't waste the little bit of space at the
	# front.  Every bit helps.

#	printf "Maybe %d; $bits[1] == 1; $bits[2] == %d; $bits[3] == %d\n", 
#   	    scalar @bits, $cylinders * 255*63 - 1, $cylinders * 255*63 - 1;
	if (-b "/dev/${drive}1" && @bits == 6 && $bits[1] =~ /^\d+$/o && $bits[1] == 1
	    && $bits[2] =~ /^\d+$/o && $bits[2] == $cylinders * 255*63 - 1 
	    && $bits[3] =~ /^\d+$/o && $bits[3] == $cylinders * 255*63 - 1) {
	    $ok{$drive} = 1;
	}
	$_ = <SFDISK>; die "??" unless m!^/dev/${drive}p?2\s+!;
	$_ = <SFDISK>; die "??" unless m!^/dev/${drive}p?3\s+!;
	$_ = <SFDISK>; die "??" unless m!^/dev/${drive}p?4\s+!;
    }
    close(SFDISK);
    return %ok;
}

sub serial_number {
    my($drive) = @_;

    my $ret;
    open(SGINFO, "sginfo -s /dev/$drive |")
	or die "bad";
    $_ = <SGINFO>;
    if (/^Serial Number '(.+)'$/) {
	$ret = $1;
    } 
    close(SGINFO);
    return $ret;
    
}

sub is_drive { # not partition
    my ($drive) = @_;

    my @stat = stat("/dev/$drive");
    die "/etc/driverdump.config pattern matched $drive, which is a partition, not a drive"
	unless 0 == ($stat[6] & 0xF);
    
}

sub usage {
    my($msg) = @_;

    print STDERR "Error: @_" if @_;
    die "Usage: $0 (partition)";
}
	
