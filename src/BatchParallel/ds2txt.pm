#
#  (c) Copyright 2005, Hewlett-Packard Development Company, LP
#
#  See the file named COPYING for license details
#

package BatchParallel::ds2txt;

@ISA = qw/BatchParallel::common/;

sub usage {
    print "batch-parallel ds2txt -- file/directory...\n";
}

sub file_is_source {
    my($this,$prefix,$fullpath,$filename) = @_;

    return 1 if $fullpath =~ /\.ds$/o;
    return 0;
}

sub destination_file {
    my($this,$prefix,$fullpath) = @_;

    $fullpath =~ s/\.ds/.txt/o;
    return $fullpath;
}

sub rebuild {
    my($this,$prefix,$fullpath,$destpath) = @_;

    print "ds2txt $fullpath >$destpath\n";
    return system("ds2txt $fullpath >$destpath") == 0;
}

1;

