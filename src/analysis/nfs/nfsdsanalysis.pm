package DataSeries::nfsdsanalysis;
# (c) Copyright 2003-2007, Hewlett-Packard Development Company, LP
#
# See the file named COPYING for license details
use strict;
use warnings;
use Exporter;
use vars qw/@ISA @EXPORT %EXPORT_TAGS/;

@ISA = qw/Exporter/;
@EXPORT = qw/&parse_NFSOpPayload &parse_FileageByFilehandle &parse_ServerLatency &parse_test/;

sub parse_NFSOpPayload {
    my($fh) = @_;

    my %op2dir2info;
    while (<$fh>) {
	last if /^End-virtual/o;
	die "AA $_" unless /^((?:TCP)|(?:UDP))\s+(\w+)\s+(\w+):\s+(\d+\.\d+) MB,\s+(\d+\.\d+) bytes.op,.*,\s+(\d+) ops$/o;
	die "?? $_" unless /^((?:TCP)|(?:UDP))\s+(\w+)\s+(\w+):\s+(\d+\.\d+) MB,\s+(\d+\.\d+) bytes.op,.*,\s+(\d+) ops\s*$/o;
	my $info = {'prot' => $1, 'op' => "$2", 'dir' => "$3",
		    'totalmb' => $4, 'avgop' => $5, 'ops' => $6};
	my $old = $op2dir2info{$info->{op}}->{$info->{dir}};
	if (defined $old) {
	    $old->{prot} = 'both';
	    die "internal" unless $old->{op} eq $info->{op} && $old->{dir} eq $info->{dir};
	    $old->{ops} += $info->{ops};
	    $old->{avgop} = ($old->{avgop}*$old->{ops} + $info->{avgop} + $info->{ops}) / ($old->{ops} + $info->{ops});
	    $old->{totalmb} += $info->{totalmb};
	} else {
	    $op2dir2info{$info->{op}}->{$info->{dir}} = $info;
	}
    }
    return \%op2dir2info;
}

sub parse_FileageByFilehandle {
    my($fh) = @_;

    my $recent_age_secs;
    my $unique_fh;
    my $recent_fh;
    my $total_fh_gb;
    my $recent_fh_gb;

    while (<$fh>) {
	last if /^End-virtual/o;
	if (/^(\d+) unique filehandles, (\d+) recent .(\d+) seconds.: (\d+\.\d+) GB total files accessed; (\d+\.\d+) GB recent, or \d+\.\d+%$/o) {
	    ($unique_fh, $recent_fh, $recent_age_secs, $total_fh_gb, 
	     $recent_fh_gb) = ($1,$2,$3,$4,$5);
	}
    }
    die "internal" unless defined $recent_age_secs;

    return { 'recent_age_secs' => $recent_age_secs,
	     'unique_fh' => $unique_fh,
	     'recent_fh' => $recent_fh,
	     'total_fh_gb' => $total_fh_gb,
	     'recent_fh_gb' => $recent_fh_gb };
}

sub parse_ServerLatency {
    my($fh) = @_;

    my %serverinfo;
    $_ = <$fh>;chomp;
    die "?? '$_'" unless $_ eq 'server operation: #ops #dups dup-mean ; firstreqlat mean 50% 90% ; lastreqlat mean 50% 90%';
    $_ = <$fh>;chomp;
    die "?? '$_'" unless /^(\d+) missing requests, (\d+) missing replies; (\d+\.\d+)ms mean est firstlat, (\d+\.\d+)ms mean est lastlat$/o;
    $serverinfo{missing} = { 'requests' => $1, 'replies' => $2, 'est_firstlat' => $3, 'est_lastlat' => $4 };
    $_ = <$fh>;chomp;
    die "?? '$_'" unless /^duplicates: (\d+) replies, (\d+) requests; delays min=(.+)ms$/o;
    $serverinfo{duplicates} = { 'replies' => $1, 'requests' => $2, 'mindelay' => $3 };
    $_ = <$fh>;chomp;
    die "?? '$_'" unless /^(\d+) data points, mean (\S+) \+\-/o;
    $serverinfo{duplicates}->{mean_req_iat} = $2;
    if ($1 > 0) {
	$_ = <$fh>; chomp;
	die "?? '$_'" unless /^\s+quantiles every (\d+) data points:$/o;
	$_ = <$fh>; chomp;
	die "?? '$_'" unless s/^\s+5%: //o;
	my @quantiles = split(/, /o,$_);
	die "?? '$_'" unless @quantiles == 10;
	$_ = <$fh>; chomp;
	die "?? '$_'" unless s/^\s+55%: //o;
	my @quantiles2 = split(/, /o,$_);
	die "?? '$_'" unless @quantiles == 10;
	push(@quantiles,@quantiles2);
	$serverinfo{duplicates}->{req_iat_quantiles} = \@quantiles;
    }
    while (<$fh>) {
	last if /^End-virtual/o;
	chomp;
	die "?a? '$_'" unless /^(\d+\.\d+\.\d+\.\d+)\s+(\w+):\s+(\d+)\s+(\d+)\s+(\d+\.\d+) ;\s+/o;
	die "?b? '$_'" unless /^(\d+\.\d+\.\d+\.\d+)\s+(\w+):\s+(\d+)\s+(\d+)\s+(\d+\.\d+) ;\s+(\d+\.\d+)\s+(\d+\.\d+)\s+(\d+\.\d+) ;/o;
	die "?? '$_'" unless /^(\d+\.\d+\.\d+\.\d+)\s+(\w+):\s+(\d+)\s+(\d+)\s+(\d+\.\d+) ;\s+(\d+\.\d+)\s+(\d+\.\d+)\s+(\d+\.\d+) ;\s+(\d+\.\d+)\s+(\d+\.\d+)\s+(\d+\.\d+)$/o;
	my($server,$op,$ops,$dups,$dup_mean_iat,$first_mean,$first_50,$first_90,$last_mean,$last_50,$last_90)
	    = ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11);
	$serverinfo{$server} ||= { 'server' => $server,
				   'sumops' => 0,
				   'sumlat' => 0,
				   'sumdups' => 0,
				   'oplat' => {} };
	my $sinfo = $serverinfo{$server};
	die "duplicate oplat $server/$op" if defined $sinfo->{oplat}->{$op};
	$sinfo->{oplat}->{$op} = { 'ops' => $ops,
				   'dups' => $dups,
				   'dup_mean_iat' => $dup_mean_iat,
				   'first_mean' => $first_mean, 
				   'first_50' => $first_50,
				   'first_90' => $first_90,
				   'last_mean' => $last_mean,
				   'last_50' => $last_50,
				   'last_90' => $last_90 };
	$sinfo->{sumops} += $ops;
	$sinfo->{sumlat} += $first_mean * $ops;
	$sinfo->{sumdups} += $dups;
    }
    return \%serverinfo;
}

1;
