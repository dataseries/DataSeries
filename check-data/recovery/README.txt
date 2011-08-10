These files are for checking the recovery script; that each of them recovered correctly was manually
checked via ds2txt and kdiff3.

original.ds	   A copy of ../nfs-2.set-0.ip.ds
corrupt*.ds	   A copy of original.ds with some corruption
recovered*.ds	   The output of dsrecover for the corresponding corrupted file.

-100-zeros-at-50K.ds : 
Using dd, overwrote 100 bytes at offset 50000 with nulls.  This corrupts one extent in the middle;
dsrecover is expected to rebuild with one extent missing.

-stomp-beginning.ds : 
Using emacs, changed one byte in the extent type library.  There is no matching recovered file; this
corruption is expected to be unrecoverable.

-stomp-end.ds : 
Using dd, overwrote the last 100 bytes with random bytes.  This corrupts the extent index extent;
dsrecover is expected to recover all of the data.

-two-extents.ds :
Using emacs, corrupted two bytes; one early in the file and one in the middle.  This corrupts two
extents; dsrecover is expected to rebuild with those two extents missing.
