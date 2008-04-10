package BatchParallel::nonmod::gpgkeys;
use strict;

sub setup_keys {
    my($keyfile) = @_;

    if (defined $ENV{NAME_KEY_1} && defined $ENV{NAME_KEY_2}) {
	print "Got encryption keys from environment.\n";
	return;
    }
    $keyfile = "$ENV{HOME}/.hash-keys.txt.gpg" unless defined $keyfile;
    if ($keyfile eq 'nokey') {
        warn "running without encryption keys...";
        return;
    }

    my $pre_decrypted = $keyfile;
    $pre_decrypted =~ s/\.gpg$//o;

    if (-r $pre_decrypted) {
	open(KEYS, $pre_decrypted) 
            || die "Can't open $pre_decrypted for read: $!";
	print "Using pre-decrypted keys from $pre_decrypted\n";
	$_ = <KEYS>;
	internalParseKeys();
	return;
    }

    die "$keyfile doesn't exist; can't setup keys"
        unless -r $keyfile;
    while(1) {
        print "Getting encrypt key from $keyfile...\n";
        open(KEYS,"gpg --decrypt $keyfile |") 
            || die "fail keys\n";
        $_ = <KEYS>;
        if (!defined $_ || $_ eq '') {
            print "Incorrect password?  Try again.\n";
            next;
        }
	internalParseKeys();
        last;
    }
}

#TODO make this parse the setenv style files also
sub internalParseKeys {
    die "?? $_\n" unless /^NAME_KEY_1=([0-9a-f]+)$/o;
    $ENV{'NAME_KEY_1'} = $1;
    $_ = <KEYS>;
    die "?? $_\n" unless /^NAME_KEY_2=([0-9a-f]+)$/o;
    $ENV{'NAME_KEY_2'} = $1;
    close(KEYS);
}

1;

