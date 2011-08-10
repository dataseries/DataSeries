#!/usr/bin/perl
die "Usage: $0 <os>-<dist>-<arch>" unless @ARGV == 1;

my $dist = $ARGV[0];
$dist =~ s/^\w+-(\w+)-\w+$/$1/o;
my $fixfn;
eval "\$fixfn = \\&$dist;";
die $@ if $@;
eval ' &$fixfn;';
if ($@ =~ /Undefined subroutine/o) {
    print STDERR "Using noop fixup function for $dist\n";
    $fixfn = \&noop;
} elsif ($@) {
    die $@;
} else {
    print STDERR "Using special fixup function $dist\n";
}

while (<STDIN>) {
    &$fixfn;
    print;
}

sub noop { }

sub karmic {
    s/^(Build-Depends: .+)/$1, libboost-math1.38-dev/o;
}
