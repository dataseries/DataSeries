#!/usr/bin/perl -w
use strict;
use Date::Parse;

die "Usage: $0 <input trace ...>\n" if @ARGV == 0 || $ARGV[0] =~ /^-h/;
foreach my $file (@ARGV) {
    die "$file isn't readable\n" unless -r $file;
}

$|=1;
print "#curtime user pid ppid time command args...\n";
foreach my $file (@ARGV) {
    if ($file =~ /\.bz2$/o) {
	open(FILE,"bunzip2 -c $file |")
	    || die "Can't bunzip2 -c $file: $!\n";
    } elsif ($file =~ /\.gz$/o) {
	open(FILE,"gunzip -c $file |")
	    || die "Can't gunzip -c $file: $!\n";
    } else {
	open(FILE,$file) || die "Can't open $file: $!\n";
    }
    my $curtime;
    my $psformat;
    while(<FILE>) {
	next if /^\s*$/o;
	chomp;
	if (/^PS starts at: (.+)$/o) {
	    my $ctime = $1;
	    $curtime = str2time($ctime);
	    die "Unable to interpret $ctime\n" unless defined $curtime;
	    if ($ctime =~ s/ PDT//o) {
		my $xtime = localtime($curtime);
		die "?! '$ctime' '$xtime'\n" unless $ctime eq $xtime;
	    } else {
		die "Unknown timezone for $ctime\n";
	    }
	} elsif (/^PS ends at: (.+)$/o) {
	    my $ctime = $1;
	    die "Got '$_' without PS starts?\n" unless defined $curtime && defined $psformat;
	    my $endtime = str2time($ctime);
	    die "Unable to interpret $ctime\n" unless defined $endtime;
	    my $delta = $endtime - $curtime;
	    warn "$delta second separation for getting ps ($curtime, $endtime = $ctime)\n"
		if $delta < 0 || $delta > 1;
	    $curtime = undef;
	    $psformat = undef;
	} elsif ($_ eq '  F S      UID   PID  PPID  C PRI NI             ADDR   SZ            WCHAN    STIME TTY       TIME COMD') {
	    die "got PS header without PS starts at:\n" unless defined $curtime;
	    $psformat = 'hpux-xxx';
	} elsif (defined $curtime && defined $psformat) {
	    if ($psformat eq 'hpux-xxx') {
		unless (/^\s*\d+ ([SRZTI])\s+(\w+)\s+(\d+)\s+(\d+)\s+\d+\s{1,2}-?\d+ {1,2}\d+\s+[a-f0-9]+\s+\d+\s+(?:(?:-)|(?:[0-9a-f]+))\s+(?:(?:\S+\s{1,2}\d+)|(?:\d+:\d+:\d+))\s{1,2}\S+\s+(\d+:\d+) (\S+)(\s+.+)?/o) {
		    die "hpux-xxx-1 bad '$_'\n" 
			unless /^\s*\d+ [SRZTI]\s+(\w+)\s+(\d+)/o;
		    die "hpux-xxx-2 bad '$_'\n" 
			unless /^\s*\d+ [SRZTI]\s+(\w+)\s+(\d+)\s+(\d+)\s+\d+/o;
		    die "hpux-xxx-3 bad '$_'\n" 
			unless /^\s*\d+ [SRZTI]\s+(\w+)\s+(\d+)\s+(\d+)\s+\d+\s{1,2}-?\d+/o;
		    die "hpux-xxx-4 bad '$_'\n"
			unless /^\s*\d+ ([SRZTI])\s+(\w+)\s+(\d+)\s+(\d+)\s+\d+\s{1,2}-?\d+ {1,2}\d+\s+[a-f0-9]+\s+\d+\s+/o;
		    die "hpux-xxx-5 bad '$_'\n"
			unless /^\s*\d+ ([SRZTI])\s+(\w+)\s+(\d+)\s+(\d+)\s+\d+\s{1,2}-?\d+ {1,2}\d+\s+[a-f0-9]+\s+\d+\s+(?:(?:-)|(?:[0-9a-f]+))\s+/o;
		    die "hpux-xxx-6 bad '$_'\n"
			unless /^\s*\d+ ([SRZTI])\s+(\w+)\s+(\d+)\s+(\d+)\s+\d+\s{1,2}-?\d+ {1,2}\d+\s+[a-f0-9]+\s+\d+\s+(?:(?:-)|(?:[0-9a-f]+))\s+(?:(?:\S+\s{1,2}\d+)|(?:\d+:\d+:\d+))/o;
		    die "Weird $psformat PS line '$_'\n";
		}
		    
		my ($state,$uid,$pid,$ppid,$time,$cmd,$args) = ($1,$2,$3,$4,$5,$6,$7);
		$args = '' unless defined $args;
		$args =~ s/^\s+//o;
		if ($state eq 'Z' || $cmd eq '<defunct>') {
		    die "bad '$_'\n" unless $state eq 'Z' && $cmd eq '<defunct>';
		    next;
		}
		die "bad time $time in '$_'\n" unless $time =~ /^(\d+):(\d+)$/o;
		$time = 60 * $1 + $2;
		print "$curtime $uid $pid $ppid $time $cmd $args\n";
	    } else {
		die "Unknown ps format $psformat\n";
	    }
	} else {
	    die "Unknown line '$_' with curtime='$curtime' and psformat='$psformat'\n";
	}
    }
}

