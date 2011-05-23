#!/usr/bin/perl -w
use strict;
use FileHandle;

die "Usage: $0 <os> <version> <release>" unless @ARGV == 3;
my ($os, $version, $release) = @ARGV;
die "? $version" unless $version =~ /^[0-9\.]+$/o;
die "? $release" unless $release =~ /^\d+$/o;

my $control = new FileHandle "debian/control"
    or die "can't open debian/control: $!";

my %descriptions;
my $package = 'unknown';
while(<$control>) {
    $package = $1 if /^Package: (\S+)$/o;
    if (/^Description: (.+)$/o) {
        my @description = ("$1\n", "\n");
        while (<$control>) {
            last if /^\s*$/o;
            s/^\s+//o;
            s/libdataseries-dev/DataSeries-devel/o;
            push(@description, $_);
        }
        $descriptions{$package} = join('', @description);
    }
}

my $in = new FileHandle "redhat/DataSeries.spec.in"
    or die "can't open redhat/DataSeries.spec.in: $!";

my $out = new FileHandle ">redhat/DataSeries.spec"
    or die "can't open redhat/DataSeries.spec for write: $!";

while (<$in>) {
    s/__VERSION__/$version/o;
    s/__RELEASE__/$release/o;
    s/^#if-$os\s+//o;
    if (/__DESCRIPTION_(\S+)__/o) {
        my $pkg = $1;
        die "missing description for $pkg" unless defined $descriptions{$pkg};
        s/__DESCRIPTION_\S+__/$descriptions{$pkg}/;
    }
    print $out $_;
}

