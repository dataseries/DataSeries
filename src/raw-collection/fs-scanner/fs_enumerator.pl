#*****************************************************************************
#*
#* File:         fs_enumerator.pl
#* Description:  File system directory scanner
#* Author:       Brad Morrey
#* Created:      Thu Jun 28 18:04:20 PDT 2007
#* Modified:     Thu Jun 28 18:04:20 PDT 2007 (Brad Morrey) brad.morrey@hpl.hp.com
#* Language:     Perl
#* Package:      N/A
#* Status:       Experimental (Do Not Distribute)
#*
#* (C) Copyright 2007, Hewlett-Packard Laboratories, all rights reserved.
#*
#****************************************************************************

#This is MESSY CODE Designed to scan a set of directories and store
#all of the metadata in rotating logfiles defined by $outFileSpec If
#one of the directories scanned is /, the last function silently
#prunes proc, dev, sys subtrees because their metadata does not seem
#to be valid and screws up the file system size totals 
#
#Requires: lzf 
#TODO: Make this work with BatchParallel, with configurable
#compression, and with configurable log rotation size


use File::Find;
use MIME::Base64;
$maxDepth = 1;
$curDepth = 0;
$entryRotateCount = 1000000;
if ($#ARGV < 1) {
    print "USAGE: perl fs_enumerator.pl OUTFILESPEC DIR [DIR ...]\n";
    print "OUTFILESPEC is the prefix for all output files\n";
    print "DIR is one or more directories to be scanned in order\n";
    exit(0);
}
@dirList = @ARGV[1..$#ARGV];
$outFileSpec = $ARGV[0];
print "argv[1] is @dirList and ARGV[0] is $outFileSpec\n";
print "startdir 0 is $dirList[0] 1 is $dirList[1]\n";

$argDirCount = 0;
$outFileCurCount = 0;
foreach $curDir (@dirList) {
    $dirCopy = $curDir;
    $dirCopy =~ s/\//\_/g;
    print "$dirCopy\n";
    #print "$outFileSpec-$dirCopy-$outFileCurCount.lzf\n";
    $outFileSpecComplete = "$outFileSpec-$dirCopy";
    rotateLogs();
    find(\&wanted, $curDir);
    print (STDERR "done $argDirCount of $#dirList $curDir\n");
    $argDirCount++;
    close(OUTFILE);
    $outFileCurCount = 0;
    $outFileEntryCount = 0;
}

sub rotateLogs {
    close(OUTFILE);
    $outFileName = "|lzf -c > $outFileSpecComplete-$outFileCurCount.lzf";
    open(OUTFILE, $outFileName) or die "couldn't open $outFileName ($!)";
    $outFileCurCount++;
}

$totalSize = 0;
$curSize = 0;
#find(\&wanted, $startDir);
sub wanted {
    #print "$_\n";
    ($dev,$ino,$mode,$nlink,$uid,$gid,$rdev,$size,
     $atime,$mtime,$ctime,$blksize,$blocks) = stat $_;
    $b64Name = encode_base64($_, "")|| die "can't encode $_ because $!";
    $b64NameLength = length $b64Name;
    $b64Dir = encode_base64($File::Find::dir, "")|| die "can't encode $File::Find::dir because $!";
    $b64DirLength = length $b64Dir;
    $printString = "$b64DirLength $b64NameLength $b64Dir $b64Name $dev $ino $mode $nlink $uid $gid $rdev $size $atime $mtime $ctime $blksize $blocks\n";
    print OUTFILE $printString;
    #print "$File::Find::dir $_ $dev $ino $mode $nlink $uid $gid $rdev $size $atime $mtime $ctime $blksize $blocks\n";
    if (-f $_) {
	$curSize += $size;
	if ($curSize > (500*1024*1024)) {
	    $totalSize += $curSize;
	    $curSize = 0;
	    $printCount = $totalSize/(1024*1024*1024);
	    print (STDERR "$printCount GB $File::Find::dir $_\n");
	}
    }
    $outFileEntryCount++;
    if ($outFileEntryCount > $entryRotateCount) {
	#Rotate
	rotateLogs();
	$outFileEntryCount = 0;
    }
    #Stuff to skip here -----
    if ($File::Find::dir =~ /^\/$/) {
	print STDERR "in root dir\n";
	if ($_ =~ /^proc$/ || $_ =~ /^dev$/ || $_ =~ /^sys$/) {
	    $File::Find::prune = 1;
	    print STDERR "PRUNING!\n";
	}
    }
    #print "\n";
}
