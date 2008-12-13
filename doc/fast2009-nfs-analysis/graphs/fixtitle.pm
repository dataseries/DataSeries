sub fixTitle {
    local($_) = @_;
    s/nfs-1/anim-2003/o;
    s/nfs-2/anim-2007/o;
    return $_;
}

1;

