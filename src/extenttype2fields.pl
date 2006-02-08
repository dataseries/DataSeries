#!/usr/bin/perl
#
#  (c) Copyright 2003-2005, Hewlett-Packard Development Company, LP
#
#  See the file named COPYING for license details
#

die "Usage: $0 <series-variable>\n"
    unless @ARGV == 1 && $ARGV[0] =~ /^\w+$/o;

my %typemap = qw/
    bool  BoolField
    int32 Int32Field
    int64 Int64Field
    double DoubleField
    variable32 Variable32Field
  /;

while(<STDIN>) {
    s/^\s*\"//o;
    s/\\n//o;
    s/\"\s*$//o;
    s/\\\"/\"/go;
    next unless /\<\s*field/o;
    next unless /type\s*=\s*\"(\w+)\"/o;
    my $type = $1;
    unless(defined $typemap{$type}) {
	warn "Unknown type $type in '$_'\n";
	next;
    }
    $type = $typemap{$type};

    next unless /name\s*=\s*\"([^\"]+)\"/o;
    my $name = $1;
    my $varname = $name;
    $varname =~ s/-/_/go;
    $varname =~ s/\s/_/go;
    $varname =~ s/:/_/go;

    my @args = ($ARGV[0],"\"$name\"");
    my @flags;
    if (/opt_nullable=\"yes\"/o) {
	push(@flags,"Field::flag_nullable");
    }
    if (@flags > 0) {
	push(@args,join("|",@flags));
    }
    print "$type $varname(",join(", ",@args),");\n";
}
