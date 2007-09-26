#!/usr/bin/perl

use Net::Ping;

# USAGE: perl hostpinger.pl INFILE OUTFILE
# INFILE is the name of the file that contains directory hostname pairs
# one per line
# OUTFILE is the name of the file that will contain all directories 
# for which the associated hostname in the infile is pingable in less
# than 1ms.  Uses a Hashtable to limit ping traffic.

print "file to process: $ARGV[0]\n";
open (DIRHOST, $ARGV[0]) or die "couldnt' open $ARGV[0] ($!)";
open (OUTDIRS, ">$ARGV[1]") or die "couldnt' open $ARGV[1] ($!)";
while (defined ($cur_dir_host = <DIRHOST>)) {
    chomp($cur_dir_host);
    #print "cur line: $cur_dir_host\n";
    @split_it_up = split /\s+/, $cur_dir_host;
    #print "count $#split_it_up\n";
    #foreach $foo (@split_it_up) {
#	print "cur $foo\n";
#    }
    $cur_dir = $split_it_up[0];
    $cur_host = $split_it_up[1];
    
    $host_ping = $host_map{$cur_host};
    print "dir $cur_dir host $cur_host\n";

    unless (defined $host_ping) {
	$p = Net::Ping->new();
	$p->hires();
	($ret, $host_ping, $ip) = $p->ping($cur_host, 5.5);
	printf("$host [ip: $ip] is alive (packet return time: %.2f ms)\n", 1000 * $host_ping)
	    if $ret;
	$p->close();
	$host_map{$cur_host} = $host_ping;
    }
    if ($host_ping * 1000 < 1) {
	print OUTDIRS $cur_dir;
	print OUTDIRS "\n";
    }
}

foreach $bar (keys %host_map) {
    print "key $bar value $host_map{$bar}\n";
}
