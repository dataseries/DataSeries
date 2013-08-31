#!/usr/bin/perl -w
#
#  (c) Copyright 2003-2005, Hewlett-Packard Development Company, LP
#
#  See the file named COPYING for license details
#

use strict;
use SSP;
use SSP::Panopticon::sys_info;
use SSP::Panopticon::diskinfo;
use Spreadsheet::WriteExcel;
use Date::Format;
use POSIX qw(floor);
use Getopt::Long;
use Data::Dumper;

my $lvpvproc_filename;
my $sysinfo_filename = 'sys_info';
my $output_filename = 'hostioinventory.xls';

my $success = GetOptions('lvpvproc=s' => \$lvpvproc_filename,
			 'sysinfo=s' => \$sysinfo_filename,
			 'output=s' => \$output_filename);

die "Usage: $0 [--lvpvproc=<lvpvprocessusage output>]
  --sysinfo=<filename, default sys_info>
  --output=<filename, default hostioinventory.xls>\n"
    unless $success && @ARGV == 0;

my $lvpvproc = defined $lvpvproc_filename ? new LVPVProc($lvpvproc_filename) : undef;
my $sysinfo = new SSP::Panopticon::sys_info($sysinfo_filename);
my $workbook = Spreadsheet::WriteExcel->new($output_filename) or die "No writexcel $ARGV[2]";

#print Dumper($sysinfo);
my $titleformat = $workbook->add_format();
$titleformat->set_bold();
$titleformat->set_align('center');

my @warnings;
make_lvusagesheet();
make_lvinfosheet();
make_pvinfosheet();
make_xpinfosheet();

if (@warnings > 0) {
    my $warningsheet = $workbook->add_worksheet('Analysis Warnings');
    my $cur_line = 2;
    $warningsheet->write(0,0,"Warnings generated during analysis",$titleformat);
    foreach $_ (@warnings) {
	$warningsheet->write($cur_line,0,$_);
	++$cur_line;
    }
}

sub make_lvusagesheet { 
    my $lvusagesheet = $workbook->add_worksheet('Dynamic LV Info');

    $lvusagesheet->write(0,0,"Dynamic summary by Logical Volume; (no usage information gathered)",$titleformat);
    write_titles($lvusagesheet,2,
		 "Volume Group","Logical volume","Size(GB)",
		 "# Accesses","MB Acceses","# Reads","MB Reads",
		 "# Writes","MB Writes","Top 3 processes (access%, mb%)"); 

    my $cur_line = 3;
    foreach my $vgname (sort keys %{$sysinfo->{vginfo}}) {
	my $vginfo = $sysinfo->{vginfo}->{$vgname};
	$lvusagesheet->write_string($cur_line,0,$vgname);
	foreach my $lvname (sort keys %{$vginfo->{lvs}}) {
	    my $lvinfo = $vginfo->{lvs}->{$lvname};
	    $lvusagesheet->write_string($cur_line,1,$lvname);
	    $lvusagesheet->write($cur_line,2,$lvinfo->{mb}/1024);
	    ++$cur_line;
	}
	++$cur_line;
    }

    return unless defined $lvpvproc;
    my $starttime = time2str("%a %h %d %H:%M:%S %Z %Y",$lvpvproc->{first_time},$sysinfo->{timezone});
    my $endtime = time2str("%a %h %d %H:%M:%S %Z %Y",$lvpvproc->{last_time},$sysinfo->{timezone});
    my $elapsed = floor($lvpvproc->{last_time} - $lvpvproc->{first_time} + 0.5);
    my $days = floor($elapsed/(24*60*60)); $elapsed -= 24*60*60 * $days;
    my $hours = floor($elapsed/(60*60)); $elapsed -= 60*60 * $hours;
    my $minutes = floor($elapsed/60 + 0.5);
    my $elapsedtime;
    if ($days > 0) { 
	$elapsedtime = sprintf("%d days, %02d:%02d",$days,$hours,$minutes);
    } else {
	$elapsedtime = sprintf("%02d:%02d",$hours,$minutes);
    }
    $lvusagesheet->write(0,0,"Dynamic summary by Logical Volume; $starttime to $endtime ($elapsedtime elapsed)",$titleformat);

    $cur_line = 3;
    foreach my $vgname (sort keys %{$sysinfo->{vginfo}}) {
	my $vginfo = $sysinfo->{vginfo}->{$vgname};
	foreach my $lvname (sort keys %{$vginfo->{lvs}}) {
	    my $lvinfo = $vginfo->{lvs}->{$lvname};
	    $lvinfo->{id} =~ s/^0x/0x40/o || die "bad id $lvinfo->{id}\n";
	    my $lvios = $lvpvproc->{lvs}->{$lvinfo->{id}};
	    $lvios = {'read' => [0,0],'write' => [0,0],'procs' => []}
	    unless defined $lvios;
	    my $accesses = $lvios->{read}->[0] + $lvios->{write}->[0];
	    my $mbs = $lvios->{read}->[1] + $lvios->{write}->[1];
	    $lvusagesheet->write($cur_line,3,$accesses);
	    $lvusagesheet->write($cur_line,4,$mbs);
	    $lvusagesheet->write($cur_line,5,$lvios->{read}->[0]);
	    $lvusagesheet->write($cur_line,6,$lvios->{read}->[1]);
	    $lvusagesheet->write($cur_line,7,$lvios->{write}->[0]);
	    $lvusagesheet->write($cur_line,8,$lvios->{write}->[1]);
	    for (my $i=0;$i<3;++$i) {
		last if $i == @{$lvios->{procs}};
		my $procinfo = $lvios->{procs}->[$i];
		$lvusagesheet->write($cur_line,9+$i,
				     sprintf("%s (%.2f,%.2f)",$procinfo->{command},
					     100*($procinfo->{read}->[0]+$procinfo->{write}->[0])/$accesses,
					     100*($procinfo->{read}->[1]+$procinfo->{write}->[1])/$mbs));
	    }
	    $lvios->{found} = 1;

	    ++$cur_line;
	}
	++$cur_line;
    }
    while (my ($lvid,$lvinfo) = each %{$lvpvproc->{lvs}}) {
	next if $lvinfo->{found};
	push(@warnings,"didn't find lvid $lvid in sys_info?!, it had $lvinfo->{read}->[0] reads and $lvinfo->{write}->[0] writes.");
    }
}

sub compress_disklist {
    my @pvs;
    foreach my $pv (@_) {
	die "?! $pv->{dev}\n"
	    unless $pv =~ /^c(\d+)t(\d+)d(\d+)$/o;
	push(@pvs,[$1,$2,$3]);
    }
    @pvs = sort { return $a->[0] <=> $b->[0] || $a->[1] <=> $b->[1] || $a->[2] <=> $b->[2] } @pvs;

    # compress disks with same controller/target together
    my @shortened;
    my ($prevc,$prevt) = (-1,-1);
    my @dlist;
    foreach my $pv (@pvs) {
	if ($pv->[0] == $prevc && $pv->[1] == $prevt) {
	    push(@dlist,$pv->[2]);
	} else {
	    if (@dlist > 0) {
		push(@shortened,[$prevc,$prevt,[@dlist]]);
	    }
	    ($prevc,$prevt) = ($pv->[0],$pv->[1]);
	    @dlist = ($pv->[2]);
	}
    }
    push(@shortened,[$prevc,$prevt,[@dlist]]);

    # compress targets with same controller/disklist together
    @pvs = @shortened;
    @shortened = ();
    my @tlist;
    my $prevd = '';
    $prevc = -1;
    foreach my $pv (@pvs) {
	if ($pv->[0] == $prevc && join(",",@{$pv->[2]}) eq $prevd) {
	    push(@tlist,$pv->[1]);
	} else {
	    if (@tlist > 0) { 
		$prevd = "{$prevd}" if $prevd =~ /,/o;
		push (@shortened,[$prevc,[@tlist],$prevd]);
	    }
	    $prevc = $pv->[0];
	    $prevd = join(",",@{$pv->[2]});
	    @tlist = ($pv->[1]);
	}
    }
    $prevd = "{$prevd}" if $prevd =~ /,/o;
    push (@shortened,[$prevc,[@tlist],$prevd]);

    @pvs = ();
    foreach my $pv (@shortened) {
	if (@{$pv->[1]} > 1) {
	    push(@pvs,"c$pv->[0]t{" . join(",",@{$pv->[1]}) . "}d$pv->[2]");
	} else {
	    push(@pvs,"c$pv->[0]t$pv->[1]->[0]d$pv->[2]");
	}
    }
    return join(", ",@pvs);
}

sub make_lvinfosheet {
    my $lvinfosheet = $workbook->add_worksheet('Static LV Information');
    
    my @c = sort keys %{$sysinfo->{vginfo}};
    $lvinfosheet->write(0,0,"Static summary by Logical Volume",$titleformat);
    write_titles($lvinfosheet,2,
		 "Volume Group","Logical volume","Size(GB)","Stripe Size (k)","Stripe #/Stripe Width","Devices...");
    
    my $cur_line = 3;
    foreach my $vgname (sort keys %{$sysinfo->{vginfo}}) {
	my $vginfo = $sysinfo->{vginfo}->{$vgname};
	$lvinfosheet->write_string($cur_line,0,$vgname);
	foreach my $lvname (sort keys %{$vginfo->{lvs}}) {
	    my $lvinfo = $vginfo->{lvs}->{$lvname};
	    $lvinfosheet->write_string($cur_line,1,$lvname);
	    $lvinfosheet->write($cur_line,2,$lvinfo->{mb}/1024);
	    if (defined $lvinfo->{nstripes} && $lvinfo->{nstripes} > 1) {
		$lvinfosheet->write($cur_line,3,$lvinfo->{stripesize});
	    } else {
		$lvinfosheet->write($cur_line,3,"n/a");
	    }
	    my %devused;

	    my $nstripes = $lvinfo->{nstripes} || 1;
	    for (my $i=0;$i<$nstripes;++$i) {
		if ($nstripes > 1) {
		    $lvinfosheet->write($cur_line,4,"$i/$nstripes");
		} else {
		    $lvinfosheet->write($cur_line,4,"n/a");
		}
		my @pvs;
		foreach my $pv (@{$lvinfo->{pvs}}) {
		    next unless $pv->{used_in_stripe}->{$i};
		    push(@pvs,$pv->{dev});
		    $devused{$pv->{dev}} = 1;
		}
		die "?? internal error\n" unless @pvs > 0;
		my $pvs = compress_disklist(@pvs);
		$lvinfosheet->write($cur_line,5,$pvs);
		++$cur_line;
	    }
	    foreach my $pv (@{$lvinfo->{pvs}}) {
		unless ($devused{$pv->{dev}}) {
		    print Dumper($lvinfo);
		    die "internal error\n";
		}
	    }
	}
	++$cur_line;
    }

}

sub make_pvinfosheet {
    my $pvinfosheet = $workbook->add_worksheet('Static PV Information');

    $pvinfosheet->write(0,0,"Static summary by Physical Volume",$titleformat);
    write_titles($pvinfosheet,2,
		 "Physical volume","size (GB)","Device type","Device ID","host path","array path","used in LVs (alternate paths in parens)");
    my $cur_line = 3;
    my %devorder;
    foreach my $dev (keys %{$sysinfo->{diskinfo}}) {
	die "?? $dev\n" unless $dev =~ m!^/dev/rdsk/c(\d+)t(\d+)d(\d+)$!o;
	$devorder{$dev} = $1 * 10000 + $2 * 100 + $3;
    } 
    my @devs = sort { $devorder{$a} <=> $devorder{$b} } keys %{$sysinfo->{diskinfo}};
    my %typemap = ('xp' => 'XP', 
		   'scsi-disk' => 'SCSI-disk',
		   'optimus' => 'FC-60');
    my %warnings;
    my %dev_to_lvname;
    foreach my $vgname (keys %{$sysinfo->{vginfo}}) {
	my $vginfo = $sysinfo->{vginfo}->{$vgname};
	foreach my $lvname (keys %{$vginfo->{lvs}}) {
	    my $lvinfo = $vginfo->{lvs}->{$lvname};
	    foreach my $pvinfo (@{$lvinfo->{pvs}}) {
		$dev_to_lvname{$pvinfo->{dev}}->{"$vgname/$lvname"} = 1;
		foreach my $rdsk (@{$vginfo->{pvs}->{$pvinfo->{dev}}->{alternates}}) {
		    $dev_to_lvname{$rdsk}->{"($vgname/$lvname)"} = 1;
		}
	    }
		
	}
    }
#    print Dumper($sysinfo->{vginfo});
    foreach my $rdsk (@devs) {
	my $devinfo = $sysinfo->{diskinfo}->{$rdsk};
	my $ioscan = $sysinfo->{ioscan}->{$rdsk};
	die "?! couldn't find device $rdsk in ioscan?!\n"
	    unless defined $ioscan;
	$rdsk =~ s!/dev/rdsk/!!o;
	my $devtype = $typemap{$devinfo->{type}} || $devinfo->{type};

	die "Bad hwpath $ioscan->{hwpath} for $rdsk\n"
	    unless $ioscan->{hwpath} =~ m!^((\d+/){2,4}\d)((\.\d+)+)!o;
	my ($host_path,$disk_path) = ($1,$3);
	$disk_path =~ s/^\.//o;
	my $array_path = '';
	if ($devtype eq 'XP') {
	    if (!defined $sysinfo->{xpinfo} && ! defined $warnings{xpinfo}) {
		push(@warnings,"No XP information in sys info file, can't cross check XP information");
		$warnings{xpinfo} = 1;
	    }
	    $devinfo->{id_number} =~ s/^0+//o;
	    $array_path = "$devinfo->{port}/$devinfo->{raid_group}/$devinfo->{cu_ldev}";
	} elsif ($devtype eq 'SCSI-disk') {
	    $array_path = $disk_path;
#	    print Dumper($devinfo);
	} elsif ($devtype eq 'FC-60') {
	    if (!defined $sysinfo->{amdsp} && ! defined $warnings{amdsp}) {
		push(@warnings,"No amdsp information in sys info file, can't determine array path");
		$warnings{amdsp} = 1;
	    }
	    $array_path = "unknown";
	} elsif ($devtype eq 'unknown') {
	    $devtype = "$devinfo->{vendor}: $devinfo->{product}";
	    $devinfo->{id_number} = "unknown";
	}
	$pvinfosheet->write($cur_line,0,$rdsk);
	$pvinfosheet->write($cur_line,1,$devinfo->{size}/(1024*1024));
	$pvinfosheet->write($cur_line,2,$devtype);
	$devinfo->{id_number} ||= '';
	$pvinfosheet->write_string($cur_line,3,$devinfo->{id_number});
	$pvinfosheet->write_string($cur_line,3,$devinfo->{id_number});
	$pvinfosheet->write($cur_line,4,$host_path);
	$pvinfosheet->write_string($cur_line,5,$array_path);
	$pvinfosheet->write($cur_line,6,join(", ",sort keys %{$dev_to_lvname{$rdsk}}));
	++$cur_line;
    }
#    print Dumper($sysinfo->{diskinfo});
}

sub make_xpinfosheet {
    my $xpinfosheet = $workbook->add_worksheet('Static XP Information');

    $xpinfosheet->write(0,0,"Static summary of XP volumes",$titleformat);
    write_titles($xpinfosheet,2,
		 "XP ID","ACP","Array Group","CU:ldev","Port/Device");
    my %summary;
    while (my($rdsk,$diskinfo) = each %{$sysinfo->{diskinfo}}) {
	next unless $diskinfo->{type} eq 'xp';
	die "?! xpinfosheet.1 $diskinfo->{raid_group}\n"
	    unless $diskinfo->{raid_group} =~ /^([1-4]):(\d{2})$/o;
	my ($acp,$ag) = ($1,$2);
	die "?! xpinfosheet.2 $diskinfo->{cu_ldev}\n"
	    unless $diskinfo->{cu_ldev} =~ /^\d:[0-9a-f]{2}$/o;
	die "?! xpinfosheet.3 $diskinfo->{port}\n"
	    unless $diskinfo->{port} =~ /^[12][ABCDJKLNP]$/o;
	$rdsk =~ s!/dev/rdsk/!!o;
	$summary{$diskinfo->{id_number}}->{$acp}->{$ag}->{$diskinfo->{cu_ldev}}->{$diskinfo->{port}} = $rdsk;
    }
    return if scalar keys %summary == 0;
    my $cur_line = 3;
    foreach my $xpid (sort { $a <=> $b } keys %summary) {
	my $xpinfo = $summary{$xpid};
	foreach my $acp (sort { $a <=> $b} keys %$xpinfo) {
	    my $acpinfo = $xpinfo->{$acp};
	    foreach my $ag (sort { $a <=> $b} keys %$acpinfo) {
		my $aginfo = $acpinfo->{$ag};
		foreach my $culdev (sort keys %$aginfo) {
		    $xpinfosheet->write($cur_line,0,$xpid);
		    $xpinfosheet->write($cur_line,1,$acp);
		    $xpinfosheet->write($cur_line,2,$ag);
		    $xpinfosheet->write($cur_line,3,$culdev);
		    my $culdevinfo = $aginfo->{$culdev};
		    my @portdevs = map { "$_/$culdevinfo->{$_}" } keys %$culdevinfo;
		    @portdevs = sort @portdevs;
		    $xpinfosheet->write($cur_line,4,join(", ",@portdevs));
		    ++$cur_line;
		}
		++$cur_line;
	    }
	    $cur_line += 1;
	}
	$cur_line += 2;
    }
}

sub write_titles {
    my ($sheet,$row,@titles) = @_;
    for (my $i = 0;$i<@titles;++$i) {
	$sheet->write($row,$i,$titles[$i],$titleformat);
    }
}

$workbook->close() or die "Error writing workbook: $!\n";

exit(0);

package LVPVProc;

use FileHandle;

sub new {
    my($class,$file) = @_;

    my $fh = new FileHandle($file) or die "can't open $file: $!\n";
    my @data = <$fh>;
    $fh->close();
    grep(s/\n//o,@data);
    if ($data[0] eq 'LVPVProcessUsage-0.2') {
	return new_0_2($class,@data);
    }
    my $i = 0;
    die "?! bad lvpv output 1\n" unless $data[$i] =~ /^LVPVProcessUsage-0.3$/o;
    ++$i;
    die "?! bad lvpv output 2; $data[$i]\n" unless $data[$i] =~ m!^first I/O (\d+\.\d+); last I/O (\d+\.\d+)$!o;
    my ($first_time,$last_time) = ($1,$2);
    ++$i;
    my @pvcmdrollup = getIdStringRollup(\$i,\@data);
    my @lvcmdrollup = getIdStringRollup(\$i,\@data);
    my @pvrollup = getIdRollup(\$i,\@data);
    my @lvrollup = getIdRollup(\$i,\@data);
    my $this = {'first_time' => $first_time,
		'last_time' => $last_time};
    bless $this, $class;
    my %lvrollup = idrollup_to_hash(@lvrollup);
    my %pvrollup = idrollup_to_hash(@pvrollup);
    add_string_rollup(\%lvrollup,3,@lvcmdrollup);
    add_string_rollup(\%pvrollup,3,@pvcmdrollup);
    $this->{lvs} = \%lvrollup;
    $this->{pvs} = \%pvrollup;
    return $this;
}

sub add_string_rollup {
    my($rollup,$topn,@strrollup) = @_;

    foreach my $i (@strrollup) { 
	my $hexid = sprintf("0x%x",$i->{id});
	die "?? $hexid\n" unless defined $rollup->{$hexid};
	push(@{$rollup->{$hexid}->{procs}},$i);
    }
    while (my($k,$v) = each %$rollup) {
	$v->{procs} = pick_topn($topn,@{$v->{procs}});
    }
}

sub pick_topn {
    my($count,@data) = @_;

    map { $_->{bytes} = $_->{read_bytes} + $_->{write_bytes} } @data;
    map { $_->{iocount} = $_->{read_count} + $_->{write_count} } @data;
    my @bybytes = sort { $b->{bytes} <=> $a->{bytes} } @data;
    my @bycount = sort { $b->{iocount} <=> $a->{iocount} } @data;
    my @ret;
    for (my $i=0;$i<@data;++$i) {
	unless (defined $bycount[$i]->{used}) {
	    push(@ret,$bycount[$i]);
	    $bycount[$i]->{used} = 1;
	}
	last if @ret == $count;
	unless (defined $bybytes[$i]->{used}) {
	    push(@ret,$bybytes[$i]);
	    $bybytes[$i]->{used} = 1;
	}
	last if @ret == $count;
    }
    map { $_->{read} = [$_->{read_count},$_->{read_bytes}/(1024*1024)];
	  $_->{write} = [$_->{write_count},$_->{write_bytes}/(1024*1024)];
	  $_->{command} = $_->{str} } @ret;
    return \@ret;
}

sub idrollup_to_hash {
    my @rollup = @_;
    my %rollup;
    foreach my $i (@rollup) {
	my $hexid = sprintf("0x%x",$i->{id});
	$rollup{$hexid} = { 'read' => [$i->{read_count}, 
				       $i->{read_bytes}/(1024*1024)],
			    'write' => [$i->{write_count}, 
					$i->{write_bytes}/(1024*1024)] }
    }
    return %rollup;
}

sub getIdStringRollup {
    my($iptr,$data) = @_;

    my $i = $$iptr;

    die "??1\n" unless $data->[$i] eq "# Extent, type='IDStringRollup'";
    ++$i;
    die "??2\n" unless $data->[$i] eq 'rollup_id rollup_string read_count read_bytes write_count write_bytes ';
    ++$i;
    my @ret;
    while ($data->[$i] !~ /^# Extent/o) {
	die "??3 $data->[$i]\n" 
	    unless $data->[$i] =~ /^(\d+) (\S+) (\d+) (\d+) (\d+) (\d+) $/o;
	push(@ret,{ 'id' => $1, 
		    'str' => $2,
		    'read_count' => $3,
		    'read_bytes' => $4,
		    'write_count' => $5,
		    'write_bytes' => $6 });
	++$i;
	last if $i == scalar @$data;
    }
    $$iptr = $i;
    return @ret;
}

sub getIdRollup {
    my($iptr,$data) = @_;

    my $i = $$iptr;

    die "??1 $i $data->[$i]\n" unless $data->[$i] eq "# Extent, type='IDRollup'";
    ++$i;
    die "??2\n" unless $data->[$i] eq 'rollup_id read_count read_bytes write_count write_bytes ';
    ++$i;
    my @ret;
    while ($data->[$i] !~ /^# Extent/o) {
	die "??3 $data->[$i]\n" unless $data->[$i] =~ /^(\d+) (\d+) (\d+) (\d+) (\d+) $/o;
	push(@ret,{ 'id' => $1, 
		    'read_count' => $2,
		    'read_bytes' => $3,
		    'write_count' => $4,
		    'write_bytes' => $5 });
	++$i;
	last if $i == scalar @$data;
    }
    $$iptr = $i;
    return @ret;
}


sub new_0_2 {
    my ($class,@data) = @_;

    my $i = 0;
    die "?! bad lvpv output 1\n" unless $data[$i] =~ /^LVPVProcessUsage-0.2$/o;
    ++$i;
    die "?! bad lvpv output 2\n" unless $data[$i] =~ m!^first I/O (\d+\.\d+); last I/O (\d+\.\d+)$!o;
    my ($first_time,$last_time) = ($1,$2);
    ++$i;
    die "?! bad lvpv output 3\n" unless $data[$i] =~ /^Rollup by PV:$/o;
    while ($data[$i] !~ /^Rollup by LV:$/o) {
	die "?! bad lvpv output\n" if $i == @data;
	++$i;
    }
    ++$i;
    my $this = {'first_time' => $first_time,
		'last_time' => $last_time};
    bless $this, $class;
    my %lvrollup;
    while ($i < @data) {
	last if $data[$i] =~ /^read \d+ ps extents, \d+ io extents$/o;
	die "?! bad lvpvoutput 1 '$data[$i]'\n" 
	    unless $data[$i] =~ m!^  lv=40([0-9a-f]+) reads=(\d+\.\d+) MB/(\d+) ios, writes=(\d+\.\d+) MB/(\d+) ios; commands:$!o;
	my($lvid,$readmb,$reads,$writemb,$writes) = ($1,$2,$3,$4,$5);
	++$i;
	my @procs;
	while($i < @data && $data[$i] =~ /^    /o) {
	    die "?! bad lvpvoutput 2x '$data[$i]'\n"
		unless $data[$i] =~ m!^    reads=(\d+\.\d+) MB/(\d+) ios writes=(\d+\.\d+) MB/(\d+) ios, command='(.+)'$!o;
	    my($procreadmb,$procread,$procwritemb,$procwrite,$command) = ($1,$2,$3,$4,$5);
	    push(@procs,{'read' => [$procread,$procreadmb],
			 'write' => [$procwrite,$procwritemb],
			 'command' => $command});
	    ++$i;
	}
	$lvrollup{"0x$lvid"} = { 'read' => [$reads,$readmb],
				 'write' => [$writes,$writemb],
				 'procs' => \@procs };
    }
    $this->{lvs} = \%lvrollup;
    return $this;
}

		  
    
