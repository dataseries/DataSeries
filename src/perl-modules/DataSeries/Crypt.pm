package DataSeries::Crypt;
#
#  (c) Copyright 2003-2007, Hewlett-Packard Development Company, LP
#
#  See the file named COPYING for license details

=pod

=head1 NAME

DataSeries::Crypt - Dataseries encryption and decryption routines

=head1 SYNOPSIS

  use DataSeries::Crypt;
  use DataSeries::Crypt 'forcedecrypt'; # Die if we can't get keys

  my $encode_1 = encode("abcdef");
  my $encode_2 = encode("ghijkl");
  
  my $decoded = decode("$encode_1/$encode_2");
  # $decoded = "abcdef/ghijkl";

=head1 DESCRIPTION

Perl module for interacting with the encrypted structures used by
cryptutil in dataseries.

=cut

use strict;
use Exporter;
use Carp;
use Crypt::Rijndael;
use Lintel::SHA1;
use Carp;
use Sys::Hostname;

use vars qw(@EXPORT @ISA);
@ISA = qw/Exporter/;
@EXPORT = qw/decode encode/;

# my %special_to_hex;
# my %hex_to_special;
my $didkeys;
my $candecrypt;

sub sha1 ($) {
    my ($data) = @_;

    my $sha1 = $Lintel::SHA1::impl_package->new();
    $sha1->add($data);
    my $digest = $sha1->digest;
    return $digest;
}

sub import {
    my $pkg = shift;
    
    if (!$didkeys) {
	$didkeys = 1;
	my $forcedecrypt = 0;
	foreach $_ (@_) {
	    $forcedecrypt = 1 if $_ eq 'forcedecrypt';
	}
	my $hostname = hostname();
	if ((defined $ENV{NAME_KEY_1}) || $forcedecrypt) {
	    initCrypt();
	    $candecrypt = 1;
	} else {
	    print STDERR "Skipping decryption decoding, no NAME_KEY_1 defined\n";
	    $candecrypt = 0;
	}
	
	if ((defined $ENV{NAME_KEY_1}) || $forcedecrypt) {
	    $candecrypt = 1;
	} else {
	    $candecrypt = 0;
	}
    }
    # TODO: pass along args
    my $tmp = $Exporter::ExportLevel;
    $Exporter::ExportLevel = 1;
    &Exporter::import($pkg);
    $Exporter::ExportLevel = $tmp;
}

my $key_1;
my $key_2;
my $cipher;
my $zeroiv;

sub initCrypt {
    return if defined $key_1;
    if (defined $ENV{'NAME_KEY_1'}) {
	$key_1 = pack("H*",$ENV{NAME_KEY_1});
	$key_2 = pack("H*",$ENV{NAME_KEY_2});
    } else {
	my $filename = "$ENV{HOME}/.hash-keys.txt.gpg";
	if (! -f $filename && -f "hash-keys.txt.gpg") {
	    $filename = "hash-keys.txt.gpg";
	}
	die "can't find $filename\n"
	    unless -f $filename;
	while(1) {
	    open(KEYS,"gpg --decrypt $filename |") || die "fail keys\n";
	    $key_1 = <KEYS>;
	    chomp $key_1;
	    $key_2 = <KEYS>;
	    chomp $key_2;
	    while (<KEYS>) { };
	    close(KEYS);
	    if ($? == 0) {
		die "?key1? '$key_1'\n" 
		    unless $key_1 =~ /^NAME_KEY_1=([0-9a-f]+)$/o ||
			$key_1 =~ /^setenv NAME_KEY_1 ([0-9a-f]+)$/o;
		$key_1 = pack("H*",$1);
		die "?key2? '$key_2'\n" 
		    unless $key_2 =~ /^NAME_KEY_2=([0-9a-f]+)$/o ||
			$key_2 =~ /^setenv NAME_KEY_2 ([0-9a-f]+)$/o;
		$key_2 = pack("H*",$1);
		last;
	    } else {
		print "Retry, bad key?!\n";
	    }
	}
    }
    $key_1 = substr($key_1,0,16);
    $cipher = new Crypt::Rijndael($key_1,Crypt::Rijndael::MODE_CBC);
    $zeroiv = pack("C",0) x 16;

    my $tmp = 'a';
    for (my $i=0;$i<255;++$i) {
	my $enc = encryptString($tmp);
	my $dec = decryptString($enc);
	die "bad" unless $dec eq $tmp && $enc ne $tmp;
	$tmp .= pack("C",$i);
    }
}

sub sqlEnc {
    my($in,$limitlen) = @_;
    return $in if $in eq 'null';
    $limitlen ||= 255;
    my $enc = encryptString($in);
    my $ret = "'" . unpack("H*",$enc) . "'";
    die "?? too long ($in) ??" if length($ret) > $limitlen;
    return $ret;
}

sub encryptString {
    my($in) = @_;

    initCrypt();
    my $tmp = $key_2 . $in;
    my $digest = sha1($tmp);
    my $minlen = length($in) + 8;
    my $totallen = $minlen + (16 - ($minlen % 16)) % 16;
    my $hmaclen = $totallen - (length($in) + 1);
    confess "bad hmaclen $hmaclen\n" unless $hmaclen >= 7;
    my $out = pack("C",$hmaclen);
    while ($hmaclen > 20) {
	$out .= " ";
	--$hmaclen;
    }
    $out .= substr($digest,0,$hmaclen);
    $out .= $in;
    $cipher->set_iv($zeroiv);
    return $cipher->encrypt($out);
}

sub decryptString {
    my($in) = @_;

    initCrypt();
    $cipher->set_iv($zeroiv);
    $in = $cipher->decrypt($in);
    my $hmaclen = unpack("C",substr($in,0,1));
    croak "bad" unless $hmaclen >= 7 && $hmaclen <= 22;
    die "bad" unless length($in) >= ($hmaclen + 1);
    my $ret = substr($in,$hmaclen + 1, length($in) - ($hmaclen + 1));
    my $tmp = $key_2 . $ret;
    my $sha_out = sha1($tmp);
    my $cmpto = 1;
    while ($hmaclen > 20) {
	die "bad" unless substr($in,$cmpto,1) eq ' ';
	++$cmpto;
	--$hmaclen;
    }
    die "bad" unless substr($in,$cmpto,$hmaclen) eq substr($sha_out,0,$hmaclen);
    return $ret;
}

my %shortmap;
my $shortcount = 0;
sub decode {
    die "??" unless $didkeys;

    if (!$candecrypt && $ENV{MDR_SHORT}) {
	return $shortmap{$_[0]} if defined $shortmap{$_[0]};
	$shortmap{$_[0]} = sprintf("str%04d",$shortcount);
	++$shortcount;
	return $shortmap{$_[0]};
    }

    return $_[0] unless $candecrypt;
    my $tmp = $_[0];
#    $tmp = $mdr{$tmp} if defined $mdr{$tmp};
    my @parts;
    while ($tmp ne '') {
	if ($tmp =~ s!^([0-9a-f]{8,10000})!!o) {
	    my $part = $1;
	    if (((length $part) % 16) == 0) {
		my $dec = eval { decryptString(pack("H*",$part)); };
		if ($@) {
		    # warn "no decode on $part: $@\n";
		    push(@parts,$part);
		} else {
		    push(@parts,$dec);
		}
	    } else {
		push(@parts,$part);
	    }
	} elsif ($tmp =~ s!^([0-9a-f]{1,7})!!o) {
	    push(@parts,$1);
	} elsif ($tmp =~ s!^([^0-9a-f]+)!!o) {
	    push(@parts,$1);
	} else {
	    die "huh? '$tmp'";
	}
    }
    return join('',@parts);
}

sub encode {
    die "??" unless $didkeys;
    return $_[0] unless $candecrypt;
#    return $_[0] if defined $mdr{$_[0]}; # already encoded
    my $enc = encryptString($_[0]);
    my $hexenc = unpack("H*",$enc);
#    return $revmdr{$hexenc} if defined $revmdr{$hexenc};
    return $hexenc;
}

# In retrospect, this was a bad idea, but it's burned into the NFS traces,
# so we should probably get it working again.

# sub prep_crypt_reverse_map {
#     die "??" unless $candecrypt;
#     %special_to_hex = ( # invert the conversion done in cryptutil.C
# 	     "honey" => "10ba0c97d572b18dcee3d6ba940f0dce",
# 	     "training" => "35cdb4301171ed10b699f3a6fac30aaf",
# 	     "molasses" => "96cb43a41a53beef1fad90f58bc1e7e0",
# 	     "unaccounted" => "482b421099353e7bb4dedf70b646d11828bcf6fa58ea55bd80e2d7ed4f181e25",
# 	     "honey-layout" => "5dd8e2867bfbd16831fcff149cc9eaaba920c3ffc58aa2fbbfb07e3425480a7b",
# 	     "honey-animation" => "b367b70c18545fffb81c6d2b6997adf3bfc9dbf18348ae8a82d36b328affbdf9",
# 	     "honey-character-development" => "e91c596e5c5d8f2e026622af17552541127d83fd6f4fbeeab1dc7862ed1a3546",
# 	     "honey-consumer-products" => "6ea83ba12a18e0ea58d94db20f47e467e847c05f27c35edd28c4242425fb73f8", 
# 	     "honey-effects" => "1f6abc86fcc2fc5e3eb8d6436fa2eca68ff876ade9017626552446a440ac6685",
# 	     "honey-effects-2" => "ac007998eac68cfa288ad64b864def2d5ecb307ad9fad950279d2eca177b0ed3",
# 	     "honey-surfacing" => "03f29e35b81d9738bca41d33e72eb11207c49777dab1e61dcef3187647736bd2",
# 	     "honey-finaling" => "5332a998856146b49c392563cfd5272d1eb2f3119d594cde0184ee7128fc3b45",
# 	     "honey-high-priority" => "32d53e9fe78dec7bee6284f9bf402026c62aca31290973341388777fbd8cf9f1",
# 		 "honey-lighting-1" => "25fb517d3e2e78c3aaef11c4460a952262ab3a38b96382b6571d9d82ec1ac5ab",
# 	     "honey-lighting-2" => "63bb7fd62cd7219ab5082ce224c69662afc153d799d9cf514f4d92cac642f674",
# 	     "honey-lighting-2a" => "2e80c9017951b0ffe692b443edfb41e561a6a81994af7820777635471e61719d",
# 	     "honey-lighting-3" => "39174e37d9d4fe7cb94e0109d7aea769fb712cba14cc5ff74fc6c52ee0ccc706",
# 	     "honey-lighting-4" => "66ef44d3224d6f643c7c4cd4329346b77e9885f42a0688caa49f8c4376946451",
# 	     "honey-lighting-5" => "75036c1cf93bcd0314b4bb58ea61c2caa46b0388a436b0583e5bfdfb3eb5becc",
# 	     "honey-lighting-6" => "f62c3f757fd13d884df168fc72e7d24755843ad387762d9db85216edd95fb73a",
# 	     "honey-lighting-7" => "4887743bce51b08944a83fcf2efa9a7ff186ca760fd6fe43f4c377893b661a07",
# 	     
# 	     "molasses-visual-development" => "1cc2aacddfef4b164596bef234a9e028478264124f2088432952064e86b0c196",
# 	     "molasses-rough-layout" => "1d37eb72ac94ec6d815be2305bd22fa7",
# 	     "molasses-final-layout" => "486dac507b357e11f0975ce44917bc5b",
# 	     "molasses-animation" => "dc2b39e1b25d86543c669270fa0b49fd",
# 	     "molasses-effects" => "7c83ce595bc0c99701a2377077c8854e",
# 	     "molasses-surfacing" => "0f5e0904d6b7f300a751ff893ca73a66",
# 	     "molasses-finaling" => "e2a109508bf3a39ac2704b909ac0342d31905dccb279a28b24e860995ee730b2",
# 	     "molasses-lighting-1" => "d3b282264bbca742c4c03b00ff6cc23a7572131a3149090392da8eea41ac4d67",
# 	     "molasses-lighting-2" => "293eabcc2332b51aee4446b41c8c516acf8ab7c7fcf99fd96d3a2970f484aa1d",
# 	     "molasses-high-priority" => "e4f0ea98d659f490bff08b586c2546c3",
# 	     
# 	     "rj-motion" => "16ed0b8e13a689839e857a567dfb672f",
# 	     "rj-light" => "932c90d17cbe596bb6beb6e2f1933ca9",
# 	     
# 	     "rr-consumerprod" => "efaf0f701a7b9be409763aa4f10abf914e63002c9a7312a6cd863292c9bdd7bb", 
# 	     "rr-finaling" => "1d108cc95d32e24ea95922df1c32416a",
# 	     "rr-honey-lighting-7" => "1565725d7f63c6191de072ebe45f44ea",
# 	     "rr-surfacing" => "2f98d23c5215f23f122afd0c01a20bee",
# 	     "rr-animation" => "3ffc22710abd6c2c57d4a146e561e1dd",
# 	     "rr-honey-lighting-3" => "4c05ede1f6721e9600a2f9186b2fb6d5",
# 	     "rr-honey-lighting-5" => "4f84041753ca3c8ddb77c5a070a54006",
# 	     "rr-molasses-lighting-1" => "61cee6ca72298760deb669e98f6224d1",
# 		 "rr-honey-lighting-6" => "6470aa6a658afe75409f0bb230599d17",
# 	     "rr-honey-lighting-1" => "6dffcb375670432e3e38f0fb25a2054a",
# 	     "rr-final-layout" => "7a4be388b2c882d50de56d2229c4ff52",
# 	     "rr-effects" => "a7dc941cba17e45c7f2ddfd1c3649563",
# 	     "rr-cycles" => "ac6a593c6a5dfa816708b7c86b73156d",
# 	     "rr-honey-lighting-4" => "b717a07384d6ded47310cc8f1407afaa",
# 	     "rr-molasses-lighting-2" => "c62d49a0a05050023d0480565e9cc5d7",
# 	     "rr-rough-layout" => "fc011b138e4cd3632eab5121b3716b0d",
# 	     );
#     while (my($k,$v) = each %special_to_hex) {
# 	$hex_to_special{$v} = $k;
#     }
# }

1;
