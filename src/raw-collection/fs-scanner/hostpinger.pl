#!/usr/bin/perl -w
#   (c) Copyright 2007, Hewlett-Packard Development Company, LP
#
#   See the file named COPYING for license details
#
# Description: Determine which set of hosts are nearby such that 
# we will only use the fs_enumerator on the "nearby" fileservers.

use strict;
use AnyDBM_File;
use FileHandle;

my %hostpinger_cache;
tie(%hostpinger_cache, 'AnyDBM_File', "hostpinger-cache", O_CREAT|O_RDWR, 0644) 
    || die "can't create hostpinger-cache: $!\n";

my $stop = 0;
$SIG{INT} = \&stop;

sub stop {
    ++$stop;
    die "force die" if $stop > 3;
    $SIG{INT} = \&stop;
};

$|=1;

sub usage {
    print "Usage: $0 infile outfile\n";
    print "Infile contains lines with a directory and hostname on them.\n";
    print "Outfile will contain all of the directories for which the\n";
    print "associated hostname in the infile is pingable in less than 1 ms.\n";
    print "Pings each host exactly one time.\n";
    die @_ if @_;
    exit(0);
}

my $max_ping_ms = 1;

usage() if @ARGV == 0 || $ARGV[0] =~ /^-h/o;
usage("incorrect number of arguments") unless @ARGV == 2;

my ($infile,$outfile) = @ARGV;
usage("infile '$infile' does not exist") unless -f $infile;
usage("outfile '$outfile' already exists") if -f $outfile;

print "reading from $infile, writing to $outfile.\n";
open (DIRHOST, $infile) or die "could not open $infile: $!";
open (OUTDIRS, ">$outfile") or die "could not open $outfile: $!";

my @dir_recheck;
my %host_map;
while (defined (my $cur_dir_host = <DIRHOST>)) {
    chomp($cur_dir_host);
    #print "cur line: $cur_dir_host\n";
    my @split_it_up = split /\s+/, $cur_dir_host;

    if (@split_it_up == 3) {
	die "?? $cur_dir_host" unless $split_it_up[2] =~ /^$split_it_up[1]/;
	pop @split_it_up;
    }
    die "?? $cur_dir_host"
	unless @split_it_up == 2;

    my($cur_dir, $cur_host) = @split_it_up;

    #print "count $#split_it_up\n";
    #foreach $foo (@split_it_up) {
#	print "cur $foo\n";
#    }
    
    my $host_ping = $host_map{$cur_host};
    print "host $cur_host: ";

    unless(defined $host_ping) {
	$host_ping = $hostpinger_cache{"ping: $cur_host"};
    }

    unless (defined $host_ping) {
	my $cmd = "ping -c 3 -i 0.5 -w 5.5 $cur_host";
	print "ping $cur_host: ";

	open(PING, "$cmd 2>&1 |")
	    or die "can't run $cmd: $!";

	my ($ret, $ip);
	$_ = <PING>;
	if (/^PING (\S+) \((\d+\.\d+\.\d+\.\d+)\) \d+\(\d+\) bytes of data.$/o) {
	    $ip = $2;
	    print " (ip = $ip)";
	} elsif (/^ping: unknown host/o) {
	    print " (unknown host)";
	} else {
	    die "?? $_";
	}

	if (defined $ip) {
	    while(<PING>) {
		if (m!^rtt min/avg/max/mdev = (\d+\.\d+)/\d+\.\d+!o) {
		    $host_ping = $1;
		    print " (ping time $host_ping)";
		}
		if (m!(\d+) received!o) {
		    print " (ok)";
		    $ret = 1 if $1 > 0;
		}
	    }
	}
	close(PING);
	if ($? == 0) {
	    die "ping ok, output not??" unless $ret;
	} else {
	    die "output ok, ping not??" if $ret;
	}

	if ($ret) {
	    print " alive\n";
	} else {
	    print " dead\n";
	}
	$host_ping = 1e6 unless $ret;
	$host_map{$cur_host} = $host_ping;
	$hostpinger_cache{"ping: $cur_host"} = $host_ping;
    }

    if ($host_ping < $max_ping_ms) {
	print "fast";
    } else {
	print "slow; skipping\n";
	next;
    }

    print " $cur_dir: ";

    if ($hostpinger_cache{"directory: $cur_dir"}) {
	print "cached-exists\n";
    } else {
	print "slow-check...";
	
	unless (testdir($cur_dir)) {
	    $hostpinger_cache{"fail-exists: $cur_dir"} ||= 0;
	    if ($hostpinger_cache{"fail-exists: $cur_dir"} >= 5) {
		print "Failed at least 5 times, assuming doesn't exist.\n";
		next;
	    }
	    # Test -d sometimes doesn't work with the automounter
	    my $dir_exists = 
		system("ls -d $cur_dir >/dev/null 2>/dev/null") == 0;
	    select(undef,undef,undef,0.5)
		unless $dir_exists;
	    
	    unless (-d $cur_dir) {
		if (-e $cur_dir) {
		    system("ls -ld $cur_dir");
		    die "?? ($dir_exists) $cur_dir; try running it again, pinger cache may fix this :(";
		}
		print "skipping $cur_dir; it does not exist\n";
		# Can't cache this case because the automounter might find it
		# next time.
		$hostpinger_cache{"fail-exists: $cur_dir"} += 1;
		push(@dir_recheck, $cur_dir);
		next;
	    }
	    print "ok, it exists.\n";
	    die "??" unless testdir($cur_dir);
	}
	print "slow found.\n";
    }

    print OUTDIRS "$cur_dir\n";
    if ($stop) {
	untie(%hostpinger_cache);
	print "Graceful shutdown\n";
        exit(0);
    }
}

if (@dir_recheck > 0) {
    foreach my $dir (@dir_recheck) {
	testdir($dir);
    }
    my $n = @dir_recheck;
    print "Long wait for re-test of $n directories\n";
    sleep(10);
}

foreach my $dir (@dir_recheck) {
    next if $hostpinger_cache{"directory: $dir"};
    print "Recheck $dir\n";
    if (testdir($dir)) {
	die "directory $dir appeared; run me again.\n";
    }
}

foreach my $host (sort keys %host_map) {
    print "host $host ping time $host_map{$host}ms\n";
}

sub testdir {
    my($dir) = @_;

    return 1 if $hostpinger_cache{"directory: $dir"};

    if (-d $dir) {
	delete $hostpinger_cache{"fail-exists: $dir"};
	$hostpinger_cache{"directory: $dir"} = 1;
	return 1;
    } else {
	return 0;
    }
}
    
