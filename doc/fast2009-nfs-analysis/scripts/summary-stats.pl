#!/usr/bin/perl -w
use strict;
use FileHandle;

my %sum_stats;
# my ($total_packets, $total_packet_loss) = (0,0);
# my ($total_tcp_rpc, $multiple_tcp_rpc) = (0,0);
foreach my $filename (@ARGV) {
    my $fh = new FileHandle $filename or die "?$filename";

    my %stats;
    while (<$fh>) {
	die "$_ ??" unless /^(\w.+) (\d+)$/o;
	$stats{$1} = $2;
    }

    foreach my $stat (qw/packet packet_loss tcp_rpc_message tcp_multiple_rpcs nfsv3_request unhandled_nfsv3_requests/) {
	die "?? $filename $stat" unless defined $stats{$stat};
	$sum_stats{$stat} += $stats{$stat};
    }
}

die "?? starting capture always reports 65535 packets lost" 
    unless $sum_stats{packet_loss} >= 65535;
$sum_stats{packet_loss} -= 65535;

print "loss: $sum_stats{packet_loss} / $sum_stats{packet}; ";
printf ("%.6f%%\n", 100 * $sum_stats{packet_loss} / $sum_stats{packet});

print "multiple/v3rpc $sum_stats{tcp_multiple_rpcs} / $sum_stats{tcp_rpc_message}; ";
printf ("%.6f%%\n", 100 * $sum_stats{tcp_multiple_rpcs} / $sum_stats{tcp_rpc_message});

print "unhandled v3req: $sum_stats{unhandled_nfsv3_requests} / $sum_stats{nfsv3_request}; ";
printf ("%.6f%%\n", 100 * $sum_stats{unhandled_nfsv3_requests} / $sum_stats{nfsv3_request});

# 50,868,373,735
    
